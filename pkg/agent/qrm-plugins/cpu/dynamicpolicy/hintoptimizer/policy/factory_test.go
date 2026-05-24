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

package policy

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/hintoptimizer"
	hintoptimizerutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/hintoptimizer/util"
)

type fakeHintOptimizer struct {
	name     string
	calls    *[]string
	err      error
	runErr   error
	runCalls int
}

func (f *fakeHintOptimizer) OptimizeHints(_ hintoptimizer.Request, _ *pluginapi.ListOfTopologyHints) error {
	if f.calls != nil {
		*f.calls = append(*f.calls, f.name)
	}
	return f.err
}

func (f *fakeHintOptimizer) Run(<-chan struct{}) error {
	f.runCalls++
	return f.runErr
}

func TestMultiHintOptimizerOptimizeHintsWithFilters(t *testing.T) {
	t.Parallel()

	request := hintoptimizer.Request{
		ResourceRequest: &pluginapi.ResourceRequest{
			PodUid:        "pod-uid",
			ContainerName: "main",
		},
	}

	for _, tt := range []struct {
		name        string
		request     hintoptimizer.Request
		optimizers  []namedHintOptimizer
		filters     []namedHintOptimizer
		wantCalls   []string
		wantErrText string
	}{
		{
			name: "new allocation runs filters first then stops on first successful configured optimizer",
			optimizers: []namedHintOptimizer{
				{name: "configured-1"},
				{name: "configured-2"},
			},
			filters: []namedHintOptimizer{
				{name: "filter-1"},
				{name: "filter-2"},
			},
			wantCalls: []string{"filter-1", "filter-2", "configured-1"},
		},
		{
			name: "filter runs first then configured skip error continues to next optimizer",
			optimizers: []namedHintOptimizer{
				{name: "configured-skip", hintOptimizer: &fakeHintOptimizer{err: hintoptimizerutil.ErrHintOptimizerSkip}},
				{name: "configured-success"},
			},
			filters: []namedHintOptimizer{
				{name: "filter"},
			},
			wantCalls: []string{"filter", "configured-skip", "configured-success"},
		},
		{
			name: "filter fatal error stops optimizers",
			filters: []namedHintOptimizer{
				{name: "filter-fatal", hintOptimizer: &fakeHintOptimizer{err: errors.New("filter failed")}},
			},
			optimizers: []namedHintOptimizer{
				{name: "configured"},
			},
			wantCalls:   []string{"filter-fatal"},
			wantErrText: "filter failed",
		},
		{
			name: "filter skip error continues to next filter",
			filters: []namedHintOptimizer{
				{name: "filter-skip", hintOptimizer: &fakeHintOptimizer{err: hintoptimizerutil.ErrHintOptimizerSkip}},
				{name: "filter-success"},
			},
			optimizers: []namedHintOptimizer{
				{name: "configured"},
			},
			wantCalls: []string{"filter-skip", "filter-success", "configured"},
		},
		{
			name: "nil filters and optimizers are skipped",
			filters: []namedHintOptimizer{
				{name: "filter-nil"},
				{name: "filter"},
			},
			optimizers: []namedHintOptimizer{
				{name: "configured-nil"},
				{name: "configured"},
			},
			wantCalls: []string{"filter", "configured"},
		},
		{
			name: "filter-only request skips configured optimizers",
			filters: []namedHintOptimizer{
				{name: "filter"},
			},
			optimizers: []namedHintOptimizer{
				{name: "configured"},
			},
			wantCalls: []string{"filter"},
			request: hintoptimizer.Request{
				ResourceRequest: &pluginapi.ResourceRequest{
					PodUid:        "pod-uid",
					ContainerName: "main",
				},
				FilterOnly: true,
			},
		},
		{
			name: "all configured optimizers skipped still runs filters first",
			filters: []namedHintOptimizer{
				{name: "filter"},
			},
			optimizers: []namedHintOptimizer{
				{name: "configured-skip", hintOptimizer: &fakeHintOptimizer{err: hintoptimizerutil.ErrHintOptimizerSkip}},
			},
			wantCalls: []string{"filter", "configured-skip"},
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			calls := []string{}
			optimizer := multiHintOptimizer{
				filters:    bindFakeHintOptimizers(tt.filters, &calls),
				optimizers: bindFakeHintOptimizers(tt.optimizers, &calls),
			}

			optimizeRequest := request
			if tt.request.ResourceRequest != nil {
				optimizeRequest = tt.request
			}
			err := optimizer.OptimizeHints(optimizeRequest, &pluginapi.ListOfTopologyHints{})
			if tt.wantErrText != "" {
				require.ErrorContains(t, err, tt.wantErrText)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantCalls, calls)
		})
	}
}

func TestMultiHintOptimizerRunIncludesFilters(t *testing.T) {
	t.Parallel()

	configured := &fakeHintOptimizer{name: "configured"}
	filter := &fakeHintOptimizer{name: "filter"}
	optimizer := multiHintOptimizer{
		optimizers: []namedHintOptimizer{
			{name: "configured", hintOptimizer: configured},
			{name: "configured-nil"},
		},
		filters: []namedHintOptimizer{
			{name: "filter", hintOptimizer: filter},
			{name: "filter-nil"},
		},
	}

	require.NoError(t, optimizer.Run(nil))
	require.Equal(t, 1, configured.runCalls)
	require.Equal(t, 1, filter.runCalls)
}

func TestMultiHintOptimizerRunAggregatesErrors(t *testing.T) {
	t.Parallel()

	optimizer := multiHintOptimizer{
		optimizers: []namedHintOptimizer{
			{name: "configured", hintOptimizer: &fakeHintOptimizer{runErr: errors.New("configured run failed")}},
		},
		filters: []namedHintOptimizer{
			{name: "filter", hintOptimizer: &fakeHintOptimizer{runErr: errors.New("filter run failed")}},
		},
	}

	err := optimizer.Run(nil)
	require.ErrorContains(t, err, "configured run failed")
	require.ErrorContains(t, err, "filter run failed")
}

func TestHintOptimizerRegistryWithFilters(t *testing.T) {
	t.Parallel()

	registry := HintOptimizerRegistry{
		"configured": func(HintOptimizerFactoryOptions) (hintoptimizer.HintOptimizer, error) {
			return &fakeHintOptimizer{}, nil
		},
		"filter": func(HintOptimizerFactoryOptions) (hintoptimizer.HintOptimizer, error) {
			return &fakeHintOptimizer{}, nil
		},
		"factory-error": func(HintOptimizerFactoryOptions) (hintoptimizer.HintOptimizer, error) {
			return nil, errors.New("factory failed")
		},
	}

	optimizer, err := registry.HintOptimizerWithFilters([]string{"configured"}, []string{"filter"}, HintOptimizerFactoryOptions{})
	require.NoError(t, err)
	multi, ok := optimizer.(*multiHintOptimizer)
	require.True(t, ok)
	require.Len(t, multi.optimizers, 1)
	require.Len(t, multi.filters, 1)

	optimizer, err = registry.HintOptimizerWithFilters([]string{"filter", "configured"}, []string{"filter"}, HintOptimizerFactoryOptions{})
	require.NoError(t, err)
	multi, ok = optimizer.(*multiHintOptimizer)
	require.True(t, ok)
	require.Len(t, multi.optimizers, 1)
	require.Equal(t, "configured", multi.optimizers[0].name)
	require.Len(t, multi.filters, 1)

	_, err = registry.HintOptimizerWithFilters([]string{"missing"}, nil, HintOptimizerFactoryOptions{})
	require.ErrorContains(t, err, "not registered")

	_, err = registry.HintOptimizerWithFilters([]string{"factory-error"}, nil, HintOptimizerFactoryOptions{})
	require.ErrorContains(t, err, "factory failed")

	_, err = registry.HintOptimizerWithFilters([]string{"configured"}, []string{"missing-filter"}, HintOptimizerFactoryOptions{})
	require.ErrorContains(t, err, "not registered")
}

func bindFakeHintOptimizers(optimizers []namedHintOptimizer, calls *[]string) []namedHintOptimizer {
	bound := make([]namedHintOptimizer, 0, len(optimizers))
	for _, optimizer := range optimizers {
		if optimizer.hintOptimizer == nil && !strings.HasSuffix(optimizer.name, "-nil") {
			optimizer.hintOptimizer = &fakeHintOptimizer{}
		}
		if fake, ok := optimizer.hintOptimizer.(*fakeHintOptimizer); ok {
			fake.name = optimizer.name
			fake.calls = calls
		}
		bound = append(bound, optimizer)
	}
	return bound
}
