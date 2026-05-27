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
	"fmt"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/hintoptimizer"
	hintoptimizerutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/hintoptimizer/util"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type HintOptimizerFactoryOptions struct {
	Conf         *config.Configuration
	MetaServer   *metaserver.MetaServer
	Emitter      metrics.MetricEmitter
	State        state.State
	ReservedCPUs machine.CPUSet
}

type HintOptimizerFactory func(options HintOptimizerFactoryOptions) (hintoptimizer.HintOptimizer, error)

type HintOptimizerRegistry map[string]HintOptimizerFactory

func (h *HintOptimizerRegistry) Register(name string, factory HintOptimizerFactory) {
	(*h)[name] = factory
}

func (h *HintOptimizerRegistry) HintOptimizer(policies []string, options HintOptimizerFactoryOptions) (hintoptimizer.HintOptimizer, error) {
	return h.HintOptimizerWithFilters(policies, nil, options)
}

func (h *HintOptimizerRegistry) HintOptimizerWithFilters(policies, filterPolicies []string, options HintOptimizerFactoryOptions) (hintoptimizer.HintOptimizer, error) {
	if len(policies) > 0 && len(filterPolicies) > 0 {
		filterSet := make(map[string]struct{}, len(filterPolicies))
		for _, name := range filterPolicies {
			filterSet[name] = struct{}{}
		}

		configuredPolicies := make([]string, 0, len(policies))
		for _, name := range policies {
			if _, ok := filterSet[name]; ok {
				general.Warningf("skip configured hint optimizer %s because it is registered as mandatory filter", name)
				continue
			}
			configuredPolicies = append(configuredPolicies, name)
		}
		policies = configuredPolicies
	}

	optimizers, err := h.newNamedHintOptimizers(policies, options)
	if err != nil {
		return nil, err
	}
	filters, err := h.newNamedHintOptimizers(filterPolicies, options)
	if err != nil {
		return nil, err
	}

	return &multiHintOptimizer{
		optimizers: optimizers,
		filters:    filters,
	}, nil
}

func (h *HintOptimizerRegistry) newNamedHintOptimizers(policies []string, options HintOptimizerFactoryOptions) ([]namedHintOptimizer, error) {
	optimizers := make([]namedHintOptimizer, 0, len(policies))
	for _, name := range policies {
		f, ok := (*h)[name]
		if !ok {
			return nil, fmt.Errorf("hint optimizer %s not registered", name)
		}

		o, err := f(options)
		if err != nil {
			return nil, fmt.Errorf("hint optimizer %s failed with error: %v", name, err)
		}

		optimizers = append(optimizers, namedHintOptimizer{
			name:          name,
			hintOptimizer: o,
		})
	}
	return optimizers, nil
}

type namedHintOptimizer struct {
	name          string
	hintOptimizer hintoptimizer.HintOptimizer
}

type multiHintOptimizer struct {
	optimizers []namedHintOptimizer
	filters    []namedHintOptimizer
}

func (m multiHintOptimizer) OptimizeHints(request hintoptimizer.Request, hints *pluginapi.ListOfTopologyHints) error {
	for _, filter := range m.filters {
		if filter.hintOptimizer == nil {
			continue
		}

		err := filter.hintOptimizer.OptimizeHints(request, hints)
		if err != nil {
			if hintoptimizerutil.IsSkipOptimizeHintsError(err) {
				general.Warningf("hint filter %s continue with error: %v", filter.name, err.Error())
				continue
			}
			return err
		}
	}

	if !request.FilterOnly {
		for _, optimizer := range m.optimizers {
			if optimizer.hintOptimizer == nil {
				continue
			}

			err := optimizer.hintOptimizer.OptimizeHints(request, hints)
			if err != nil {
				if hintoptimizerutil.IsSkipOptimizeHintsError(err) {
					general.Warningf("hint optimizer %s continue with error: %v", optimizer.name, err.Error())
					continue
				}
				return err
			}
			// if no error, return directly
			return nil
		}
	}

	return nil
}

func (m multiHintOptimizer) Run(stopCh <-chan struct{}) error {
	var errList []error
	for _, optimizer := range m.optimizers {
		if optimizer.hintOptimizer == nil {
			continue
		}

		err := optimizer.hintOptimizer.Run(stopCh)
		if err != nil {
			errList = append(errList, err)
		}
	}
	for _, filter := range m.filters {
		if filter.hintOptimizer == nil {
			continue
		}

		err := filter.hintOptimizer.Run(stopCh)
		if err != nil {
			errList = append(errList, err)
		}
	}
	return utilerrors.NewAggregate(errList)
}
