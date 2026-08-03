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

package state

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type targetRepository interface {
	ReadonlyState
	PrepareDurableTarget() (*TargetState, error)
	CommitTarget(*TargetState) error
}

var _ targetRepository = State(nil)

func TestStateInterfaceExposesOnlyReadonlyPrepareAndCommit(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "state.go", nil, 0)
	require.NoError(t, err)

	var stateInterface *ast.InterfaceType
	for _, declaration := range file.Decls {
		generalDeclaration, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, specification := range generalDeclaration.Specs {
			typeSpecification, ok := specification.(*ast.TypeSpec)
			if !ok || typeSpecification.Name.Name != "State" {
				continue
			}
			stateInterface, ok = typeSpecification.Type.(*ast.InterfaceType)
			require.True(t, ok)
		}
	}
	require.NotNil(t, stateInterface)

	var embedded []string
	var methods []string
	for _, field := range stateInterface.Methods.List {
		if len(field.Names) == 0 {
			identifier, ok := field.Type.(*ast.Ident)
			require.True(t, ok, "State must embed only named interfaces")
			embedded = append(embedded, identifier.Name)
			continue
		}
		for _, name := range field.Names {
			methods = append(methods, name.Name)
		}
	}

	require.Equal(t, []string{"ReadonlyState"}, embedded)
	require.ElementsMatch(t, []string{"PrepareDurableTarget", "CommitTarget"}, methods)
}

type recordingCheckpointManager struct {
	createCalls int
	createErr   error
	checkpoint  []byte
}

func (m *recordingCheckpointManager) CreateCheckpoint(_ string, cp checkpointmanager.Checkpoint) error {
	m.createCalls++
	if m.createErr != nil {
		return m.createErr
	}
	data, err := cp.MarshalCheckpoint()
	if err != nil {
		return err
	}
	m.checkpoint = append([]byte(nil), data...)
	return nil
}

func (m *recordingCheckpointManager) GetCheckpoint(_ string, cp checkpointmanager.Checkpoint) error {
	if len(m.checkpoint) == 0 {
		return errors.New("checkpoint not found")
	}
	return cp.UnmarshalCheckpoint(m.checkpoint)
}

func (m *recordingCheckpointManager) RemoveCheckpoint(_ string) error {
	m.checkpoint = nil
	return nil
}

func (m *recordingCheckpointManager) ListCheckpoints() ([]string, error) {
	return nil, nil
}

func newDurableTargetFixture(t *testing.T) (*stateCheckpoint, *recordingCheckpointManager, *TargetState) {
	t.Helper()

	topology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	target := &TargetState{
		PodEntries: PodEntries{
			"pod": {
				"container": &AllocationInfo{
					AllocationMeta: commonstate.AllocationMeta{
						PodUid:        "pod",
						ContainerName: "container",
						Labels:        map[string]string{"source": "base"},
					},
				},
			},
		},
		MachineState:                               GetDefaultMachineState(topology),
		NUMAHeadroom:                               map[int]float64{0: 1.5},
		AllowSharedCoresOverlapReclaimedCores:      true,
		DisableDedicatedCoresOverlapReclaimedCores: true,
	}
	cache := NewCPUPluginState(topology)
	cache.replaceOwnedTarget(target.Clone())
	manager := &recordingCheckpointManager{}
	return &stateCheckpoint{
		cache:             cache,
		cacheDurable:      true,
		policyName:        policyName,
		checkpointManager: manager,
		checkpointName:    cpuPluginStateFileName,
		emitter:           metrics.DummyMetrics{},
		topology:          topology,
	}, manager, target
}

func TestTargetStateCloneOwnsAllMutableFields(t *testing.T) {
	_, _, target := newDurableTargetFixture(t)

	cloned := target.Clone()
	cloned.PodEntries["pod"]["container"].Labels["source"] = "clone"
	cloned.MachineState[0].PodEntries["new-pod"] = ContainerEntries{}
	cloned.NUMAHeadroom[0] = 9.5

	require.Equal(t, "base", target.PodEntries["pod"]["container"].Labels["source"])
	require.NotContains(t, target.MachineState[0].PodEntries, "new-pod")
	require.Equal(t, 1.5, target.NUMAHeadroom[0])
	require.True(t, cloned.AllowSharedCoresOverlapReclaimedCores)
	require.True(t, cloned.DisableDedicatedCoresOverlapReclaimedCores)
	require.Nil(t, (*TargetState)(nil).Clone())
}

func TestLegacyCheckpointStillLoadsIntoTargetState(t *testing.T) {
	sc, _, _ := newDurableTargetFixture(t)
	sc.GenerateMachineStateFromPodEntries = func(_ *machine.CPUTopology, _ PodEntries, origin NUMANodeMap) (NUMANodeMap, error) {
		return origin, nil
	}
	legacy := []byte(`{
		"policyName":"dynamic",
		"machineState":{},
		"numa_headroom":{"0":2.5},
		"pod_entries":{},
		"allow_shared_cores_overlap_reclaimed_cores":true
	}`)
	checkpoint := NewCPUPluginCheckpoint()
	require.NoError(t, checkpoint.UnmarshalCheckpoint(legacy))
	require.NoError(t, checkpoint.VerifyChecksum())

	changed, err := sc.RestoreState(checkpoint)
	require.NoError(t, err)
	require.False(t, changed)

	target, err := sc.PrepareDurableTarget()
	require.NoError(t, err)
	require.Equal(t, map[int]float64{0: 2.5}, target.NUMAHeadroom)
	require.True(t, target.AllowSharedCoresOverlapReclaimedCores)
	require.False(t, target.DisableDedicatedCoresOverlapReclaimedCores)
}

func TestCPUPluginStateSnapshotAndReplaceOwnedTarget(t *testing.T) {
	sc, _, target := newDurableTargetFixture(t)

	snapshot := sc.cache.snapshot()
	snapshot.NUMAHeadroom[0] = 7.5
	require.Equal(t, 1.5, sc.cache.GetNUMAHeadroom()[0])

	replacement := target.Clone()
	replacement.NUMAHeadroom[0] = 3.5
	sc.cache.replaceOwnedTarget(replacement)
	require.Equal(t, 3.5, sc.cache.GetNUMAHeadroom()[0])

	replacement.NUMAHeadroom[0] = 4.5
	require.Equal(t, 4.5, sc.cache.GetNUMAHeadroom()[0], "replaceOwnedTarget must take ownership without cloning")
}

func TestPrepareDurableTargetWritesOnlyDirtyCache(t *testing.T) {
	sc, manager, _ := newDurableTargetFixture(t)

	base, err := sc.PrepareDurableTarget()
	require.NoError(t, err)
	require.Zero(t, manager.createCalls)
	require.Equal(t, sc.cache.snapshot(), base)

	sc.cache.SetNUMAHeadroom(map[int]float64{0: 2.5})
	sc.cacheDurable = false
	require.False(t, sc.cacheDurable)
	base, err = sc.PrepareDurableTarget()
	require.NoError(t, err)
	require.Equal(t, 1, manager.createCalls)
	require.True(t, sc.cacheDurable)
	require.Equal(t, 2.5, base.NUMAHeadroom[0])

	restored := NewCPUPluginCheckpoint()
	require.NoError(t, restored.UnmarshalCheckpoint(manager.checkpoint))
	require.Equal(t, base.NUMAHeadroom, restored.NUMAHeadroom)
}

func TestPrepareDurableTargetFailureKeepsCacheDirty(t *testing.T) {
	sc, manager, _ := newDurableTargetFixture(t)
	sc.cache.SetNUMAHeadroom(map[int]float64{0: 2.5})
	sc.cacheDurable = false
	manager.createErr = errors.New("write failed")

	base, err := sc.PrepareDurableTarget()
	require.EqualError(t, err, "write failed")
	require.Nil(t, base)
	require.False(t, sc.cacheDurable)
	require.Equal(t, 2.5, sc.cache.GetNUMAHeadroom()[0])
}

func TestCommitTargetPublishesOnlyAfterCheckpointSucceeds(t *testing.T) {
	sc, manager, target := newDurableTargetFixture(t)
	require.NoError(t, sc.CommitTarget(target))
	baseCheckpoint := append([]byte(nil), manager.checkpoint...)

	next := target.Clone()
	next.NUMAHeadroom[0] = 8.5
	manager.createErr = errors.New("write failed")
	require.EqualError(t, sc.CommitTarget(next), "write failed")
	require.Equal(t, 1.5, sc.cache.GetNUMAHeadroom()[0])
	require.Equal(t, baseCheckpoint, manager.checkpoint)

	manager.createErr = nil
	require.NoError(t, sc.CommitTarget(next))
	require.True(t, sc.cacheDurable)
	require.Equal(t, 8.5, sc.cache.GetNUMAHeadroom()[0])
	next.NUMAHeadroom[0] = 9.5
	require.Equal(t, 8.5, sc.cache.GetNUMAHeadroom()[0], "commit must defensively clone caller-owned target")
}
