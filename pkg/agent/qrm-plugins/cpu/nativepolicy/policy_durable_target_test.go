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

package nativepolicy

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type recordingTargetRepository struct {
	state.State
	prepareCalls int
	commitCalls  int
}

type failOnceCommitRepository struct {
	state.State
	commitErr   error
	commitCalls int
}

func (r *failOnceCommitRepository) CommitTarget(target *state.TargetState) error {
	r.commitCalls++
	if r.commitCalls == 1 {
		return r.commitErr
	}
	return r.State.CommitTarget(target)
}

func (r *recordingTargetRepository) PrepareDurableTarget() (*state.TargetState, error) {
	r.prepareCalls++
	return r.State.PrepareDurableTarget()
}

func (r *recordingTargetRepository) CommitTarget(target *state.TargetState) error {
	r.commitCalls++
	return r.State.CommitTarget(target)
}

func TestNativePolicyAllocateAndRemoveUseOneDurableCommitEach(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)

	policy, err := getTestNativePolicy(topology, t.TempDir())
	require.NoError(t, err)
	repository := &recordingTargetRepository{State: policy.state}
	policy.state = repository

	req := &pluginapi.ResourceRequest{
		PodUid:        "pod",
		PodNamespace:  "namespace",
		PodName:       "pod",
		ContainerName: "container",
		ContainerType: pluginapi.ContainerType_MAIN,
		ResourceName:  string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{
			string(v1.ResourceCPU): 2,
		},
		NativeQosClass: string(v1.PodQOSGuaranteed),
	}

	_, err = policy.Allocate(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, 1, repository.prepareCalls)
	require.Equal(t, 1, repository.commitCalls)
	require.NotNil(t, policy.state.GetAllocationInfo(req.PodUid, req.ContainerName))

	repository.prepareCalls = 0
	repository.commitCalls = 0
	_, err = policy.RemovePod(context.Background(), &pluginapi.RemovePodRequest{PodUid: req.PodUid})
	require.NoError(t, err)
	require.Equal(t, 1, repository.prepareCalls)
	require.Equal(t, 1, repository.commitCalls)
	require.Nil(t, policy.state.GetAllocationInfo(req.PodUid, req.ContainerName))
}

func TestNativePolicyAllocatePreservesExistingAllocationWhenExpansionCommitFails(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)

	policy, err := getTestNativePolicy(topology, t.TempDir())
	require.NoError(t, err)
	req := &pluginapi.ResourceRequest{
		PodUid:        "pod",
		PodNamespace:  "namespace",
		PodName:       "pod",
		ContainerName: "container",
		ContainerType: pluginapi.ContainerType_MAIN,
		ResourceName:  string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{
			string(v1.ResourceCPU): 2,
		},
		NativeQosClass: string(v1.PodQOSGuaranteed),
	}

	_, err = policy.Allocate(context.Background(), req)
	require.NoError(t, err)
	original := policy.state.GetAllocationInfo(req.PodUid, req.ContainerName)
	require.NotNil(t, original)

	commitErr := errors.New("checkpoint write failed")
	repository := &failOnceCommitRepository{
		State:     policy.state,
		commitErr: commitErr,
	}
	policy.state = repository
	req.ResourceRequests[string(v1.ResourceCPU)] = 4

	_, err = policy.Allocate(context.Background(), req)
	require.ErrorIs(t, err, commitErr)
	require.Equal(t, 1, repository.commitCalls)

	preserved := policy.state.GetAllocationInfo(req.PodUid, req.ContainerName)
	require.NotNil(t, preserved)
	require.Equal(t, original.AllocationResult, preserved.AllocationResult)
	require.Equal(t, original.OriginalAllocationResult, preserved.OriginalAllocationResult)
}

func TestNativePolicyAllocateDoesNotPublishNewAllocationWhenCommitFails(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)

	policy, err := getTestNativePolicy(topology, t.TempDir())
	require.NoError(t, err)
	commitErr := errors.New("checkpoint write failed")
	repository := &failOnceCommitRepository{
		State:     policy.state,
		commitErr: commitErr,
	}
	policy.state = repository
	req := &pluginapi.ResourceRequest{
		PodUid:        "new-pod",
		PodNamespace:  "namespace",
		PodName:       "new-pod",
		ContainerName: "container",
		ContainerType: pluginapi.ContainerType_MAIN,
		ResourceName:  string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{
			string(v1.ResourceCPU): 2,
		},
		NativeQosClass: string(v1.PodQOSGuaranteed),
	}

	_, err = policy.Allocate(context.Background(), req)
	require.ErrorIs(t, err, commitErr)
	require.Equal(t, 1, repository.commitCalls)
	require.Nil(t, policy.state.GetAllocationInfo(req.PodUid, req.ContainerName))
}

func TestNativePolicyDoesNotUseLegacyStateWriters(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)

	forbidden := map[string]struct{}{
		"SetAllocationInfo": {},
		"SetPodEntries":     {},
		"SetMachineState":   {},
		"StoreState":        {},
	}
	files, err := filepath.Glob(filepath.Join(filepath.Dir(filename), "*.go"))
	require.NoError(t, err)

	for _, file := range files {
		if filepath.Ext(file) != ".go" || filepath.Base(file) == filepath.Base(filename) ||
			len(file) >= len("_test.go") && file[len(file)-len("_test.go"):] == "_test.go" {
			continue
		}
		parsed, parseErr := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		require.NoError(t, parseErr)
		ast.Inspect(parsed, func(node ast.Node) bool {
			selector, isSelector := node.(*ast.SelectorExpr)
			if !isSelector {
				return true
			}
			if _, found := forbidden[selector.Sel.Name]; found {
				t.Errorf("%s still calls legacy state writer %s", filepath.Base(file), selector.Sel.Name)
			}
			return true
		})
	}
}
