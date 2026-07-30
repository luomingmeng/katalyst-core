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

package cpulist

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/sets"

	bulkheadapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/api"
	bulkheadutils "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils"
	cpusetutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/util"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	qrmresctrl "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/resctrl"
	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type fakeCPUListManager struct {
	clos     []Clos
	writes   []cpuListWrite
	writeErr error
}

type cpuListWrite struct {
	closID string
	target string
}

func (m *fakeCPUListManager) ListManagedClos(context.Context) ([]Clos, error) {
	return append([]Clos(nil), m.clos...), nil
}

func (m *fakeCPUListManager) ApplyCPUList(_ context.Context, closID, target string) error {
	m.writes = append(m.writes, cpuListWrite{closID: closID, target: target})
	return m.writeErr
}

func newTestPlugin(manager *fakeCPUListManager) *CPUListPlugin {
	return NewCPUListPluginWithManager(&qrmresctrl.ResctrlConfig{
		CPUSetPoolToSharedSubgroup: map[string]int{
			"share-a": 3,
			"share-b": 3,
		},
		DefaultSharedSubgroup: -1,
	}, manager)
}

func handlerContext(view *bulkheadutils.CPUSetPartitionView) bulkheadapi.HandlerContext {
	return bulkheadapi.HandlerContext{View: view}
}

func TestCPUListPluginSharedPoolsUnionForSameClos(t *testing.T) {
	manager := &fakeCPUListManager{clos: []Clos{{ID: "share-03", Epoch: 1}}}
	plugin := newTestPlugin(manager)
	view := &bulkheadutils.CPUSetPartitionView{
		SharePoolMap: map[string]machine.CPUSet{
			"share-a": machine.NewCPUSet(1, 2),
			"share-b": machine.NewCPUSet(3, 4),
		},
	}

	require.NoError(t, plugin.CPUSetAdjustmentHandler(context.Background(), handlerContext(view)))
	require.Equal(t, []cpuListWrite{{closID: "share-03", target: "1-4"}}, manager.writes)
}

func TestCPUListPluginDisabledByTopLevelDisableRDT(t *testing.T) {
	plugin := newTestPlugin(&fakeCPUListManager{})
	conf := dynamicconfig.NewConfiguration()
	conf.AdminQoSConfiguration.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig.EnableCPUList = true
	conf.AdminQoSConfiguration.QRMPluginConfiguration.RDTConfig.DisableRDT = true

	require.False(t, plugin.Enable(bulkheadapi.HandlerContext{
		CPUSetAdjustmentHandlerCtx: cpusetutil.CPUSetAdjustmentHandlerCtx{DynamicConf: conf},
	}))
}

func TestCPUListPluginWritesDedicatedTarget(t *testing.T) {
	manager := &fakeCPUListManager{clos: []Clos{{ID: "dedicated", Epoch: 1}}}
	plugin := newTestPlugin(manager)
	view := &bulkheadutils.CPUSetPartitionView{Dedicated: machine.NewCPUSet(5, 6)}

	require.NoError(t, plugin.CPUSetAdjustmentHandler(context.Background(), handlerContext(view)))
	require.Equal(t, []cpuListWrite{{closID: "dedicated", target: "5-6"}}, manager.writes)
}

func TestCPUListPluginAppliesMissingTargetClos(t *testing.T) {
	manager := &fakeCPUListManager{}
	plugin := newTestPlugin(manager)
	view := &bulkheadutils.CPUSetPartitionView{Dedicated: machine.NewCPUSet(5, 6)}

	require.NoError(t, plugin.CPUSetAdjustmentHandler(context.Background(), handlerContext(view)))
	require.Equal(t, []cpuListWrite{{closID: "dedicated", target: "5-6"}}, manager.writes)
}

func TestCPUListPluginBuildTargetsSkipsEmptyCPUSets(t *testing.T) {
	plugin := newTestPlugin(&fakeCPUListManager{})
	view := &bulkheadutils.CPUSetPartitionView{
		SharePoolMap: map[string]machine.CPUSet{
			"share-a": machine.NewCPUSet(),
			"share-b": machine.NewCPUSet(1),
		},
		Dedicated: machine.NewCPUSet(),
	}

	require.Equal(t, map[string]string{"share-03": "1"}, plugin.buildTargets(view))
}

func TestCPUListPluginClearsEmptyTarget(t *testing.T) {
	manager := &fakeCPUListManager{clos: []Clos{
		{ID: consts.ResctrlGroupDedicated, Epoch: 1},
		{ID: consts.ResctrlGroupReclaim, Epoch: 1},
	}}
	plugin := newTestPlugin(manager)

	require.NoError(t, plugin.CPUSetAdjustmentHandler(context.Background(), handlerContext(&bulkheadutils.CPUSetPartitionView{})))
	require.Equal(t, []cpuListWrite{
		{closID: consts.ResctrlGroupDedicated, target: ""},
	}, manager.writes)
}

func TestCPUListPluginSkipsUnchangedCanonicalTarget(t *testing.T) {
	manager := &fakeCPUListManager{clos: []Clos{{ID: "dedicated", Epoch: 1}}}
	plugin := newTestPlugin(manager)
	view := &bulkheadutils.CPUSetPartitionView{Dedicated: machine.NewCPUSet(3, 1, 2)}

	require.NoError(t, plugin.CPUSetAdjustmentHandler(context.Background(), handlerContext(view)))
	require.NoError(t, plugin.CPUSetAdjustmentHandler(context.Background(), handlerContext(view)))
	require.Equal(t, []cpuListWrite{{closID: "dedicated", target: "1-3"}}, manager.writes)
}

func TestCPUListPluginRetriesFailedWrite(t *testing.T) {
	manager := &fakeCPUListManager{
		clos:     []Clos{{ID: "dedicated", Epoch: 1}},
		writeErr: errors.New("write cpu_list"),
	}
	plugin := newTestPlugin(manager)
	view := &bulkheadutils.CPUSetPartitionView{Dedicated: machine.NewCPUSet(1)}

	require.Error(t, plugin.CPUSetAdjustmentHandler(context.Background(), handlerContext(view)))
	manager.writeErr = nil
	require.NoError(t, plugin.CPUSetAdjustmentHandler(context.Background(), handlerContext(view)))
	require.Equal(t, []cpuListWrite{
		{closID: "dedicated", target: "1"},
		{closID: "dedicated", target: "1"},
	}, manager.writes)
}

func TestCPUListPluginRewritesRecreatedClos(t *testing.T) {
	manager := &fakeCPUListManager{clos: []Clos{{ID: "dedicated", Epoch: 1}}}
	plugin := newTestPlugin(manager)
	view := &bulkheadutils.CPUSetPartitionView{Dedicated: machine.NewCPUSet(1)}

	require.NoError(t, plugin.CPUSetAdjustmentHandler(context.Background(), handlerContext(view)))
	manager.clos = []Clos{{ID: "dedicated", Epoch: 2}}
	require.NoError(t, plugin.CPUSetAdjustmentHandler(context.Background(), handlerContext(view)))
	require.Equal(t, []cpuListWrite{
		{closID: "dedicated", target: "1"},
		{closID: "dedicated", target: "1"},
	}, manager.writes)
}

func TestCPUListPluginDisabledTransitionClearsManagedClos(t *testing.T) {
	manager := &fakeCPUListManager{clos: []Clos{
		{ID: consts.ResctrlGroupDedicated, Epoch: 1},
		{ID: consts.ResctrlGroupReclaim, Epoch: 1},
	}}
	plugin := NewCPUListPluginWithManager(&qrmresctrl.ResctrlConfig{}, manager)

	require.NoError(t, plugin.CPUSetAdjustmentDisabledHandler(context.Background(), handlerContext(nil)))
	require.Equal(t, []cpuListWrite{
		{closID: consts.ResctrlGroupDedicated, target: ""},
	}, manager.writes)
}

func TestCPUListPluginDisabledTransitionPreservesExternalClosWithoutExplicitSkip(t *testing.T) {
	manager := &fakeCPUListManager{clos: []Clos{
		{ID: "external", Epoch: 1},
		{ID: "custom", Epoch: 1},
	}}
	plugin := NewCPUListPluginWithManager(&qrmresctrl.ResctrlConfig{}, manager)

	require.NoError(t, plugin.CPUSetAdjustmentDisabledHandler(context.Background(), handlerContext(nil)))
	require.Empty(t, manager.writes)
}

func TestCPUListPluginDisabledTransitionDoesNotUseSkipCleanupAsWriteProtection(t *testing.T) {
	manager := &fakeCPUListManager{clos: []Clos{
		{ID: consts.ResctrlGroupDedicated, Epoch: 1},
		{ID: "share-03", Epoch: 1},
	}}
	plugin := NewCPUListPluginWithManager(&qrmresctrl.ResctrlConfig{
		CPUSetPoolToSharedSubgroup: map[string]int{"share-a": 3},
		SkipCleanupClosIDs:         sets.NewString("share-03"),
	}, manager)

	require.NoError(t, plugin.CPUSetAdjustmentDisabledHandler(context.Background(), handlerContext(nil)))
	require.Equal(t, []cpuListWrite{
		{closID: consts.ResctrlGroupDedicated, target: ""},
		{closID: "share-03", target: ""},
	}, manager.writes)
}

func TestCPUListPluginScopesSharedClosToConfiguredSubgroups(t *testing.T) {
	clos := []Clos{
		{ID: consts.ResctrlGroupDedicated, Epoch: 1},
		{ID: consts.ResctrlGroupReclaim, Epoch: 1},
		{ID: consts.ResctrlGroupSystem, Epoch: 1},
		{ID: consts.ResctrlGroupShare, Epoch: 1},
		{ID: "custom-default", Epoch: 1},
		{ID: "share-03", Epoch: 1},
		{ID: "share-05", Epoch: 1},
		{ID: "share-99", Epoch: 1},
		{ID: "shared-07", Epoch: 1},
		{ID: "shared-foreign", Epoch: 1},
	}
	config := &qrmresctrl.ResctrlConfig{
		CPUSetPoolToSharedSubgroup: map[string]int{"share-a": 3},
		DefaultSharedSubgroup:      5,
		DefaultClosIDs:             []string{"custom-default"},
	}

	t.Run("normal reconcile", func(t *testing.T) {
		manager := &fakeCPUListManager{clos: clos}
		plugin := NewCPUListPluginWithManager(config, manager)

		require.NoError(t, plugin.CPUSetAdjustmentHandler(context.Background(), handlerContext(
			&bulkheadutils.CPUSetPartitionView{
				Dedicated: machine.NewCPUSet(1),
				SharePoolMap: map[string]machine.CPUSet{
					"share-a":  machine.NewCPUSet(2),
					"unmapped": machine.NewCPUSet(3),
				},
			},
		)))
		require.Equal(t, []cpuListWrite{
			{closID: consts.ResctrlGroupDedicated, target: "1"},
			{closID: consts.ResctrlGroupShare, target: ""},
			{closID: "share-03", target: "2"},
			{closID: "share-05", target: "3"},
		}, manager.writes)
	})

	t.Run("disabled transition", func(t *testing.T) {
		manager := &fakeCPUListManager{clos: clos}
		plugin := NewCPUListPluginWithManager(config, manager)

		require.NoError(t, plugin.CPUSetAdjustmentDisabledHandler(context.Background(), handlerContext(nil)))
		require.Equal(t, []cpuListWrite{
			{closID: consts.ResctrlGroupDedicated, target: ""},
			{closID: consts.ResctrlGroupShare, target: ""},
			{closID: "share-03", target: ""},
			{closID: "share-05", target: ""},
		}, manager.writes)
	})
}
