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

package manager

import (
	"fmt"
	"sync"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/strategy/allocate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/strategy/allocate/registry"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/strategy/allocate/strategies/allocation"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

const (
	allocationStrategyNameDefault = "default"
)

// StrategyManager manages the selection of allocation strategies based on resource names
type StrategyManager struct {
	*registry.StrategyRegistry

	// Mapping from resource name to strategy name
	resourceToStrategy map[string]string

	// Default strategy to use when no specific strategy is configured
	defaultStrategy string

	// Mutex for thread-safe access
	mutex sync.RWMutex
}

// NewStrategyManager creates a new strategy manager
func NewStrategyManager() *StrategyManager {
	return &StrategyManager{
		StrategyRegistry:   registry.NewStrategyRegistry(),
		resourceToStrategy: make(map[string]string),
		defaultStrategy:    allocationStrategyNameDefault,
	}
}

// RegisterStrategyForResource registers a strategy for a specific resource name
func (m *StrategyManager) RegisterStrategyForResource(resourceName, strategyName string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Check if the strategy exists
	_, err := m.GetAllocationStrategy(strategyName)
	if err != nil {
		return fmt.Errorf("strategy %s not found: %v", strategyName, err)
	}

	m.resourceToStrategy[resourceName] = strategyName
	general.Infof("Registered strategy %s for resource %s", strategyName, resourceName)
	return nil
}

func (m *StrategyManager) GetDefaultStrategy() (allocate.AllocationStrategy, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return m.GetAllocationStrategy(m.defaultStrategy)
}

// SetDefaultStrategy sets the default strategy to use when no specific strategy is configured
func (m *StrategyManager) SetDefaultStrategy(strategyName string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Check if the strategy exists
	_, err := m.GetAllocationStrategy(strategyName)
	if err != nil {
		return fmt.Errorf("strategy %s not found: %v", strategyName, err)
	}

	m.defaultStrategy = strategyName
	general.Infof("Set default strategy to %s", strategyName)
	return nil
}

// GetStrategyForResource returns the strategy name for a given resource
func (m *StrategyManager) GetStrategyForResource(resourceName string) string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if strategyName, exists := m.resourceToStrategy[resourceName]; exists {
		return strategyName
	}

	return m.defaultStrategy
}

// getAllocationStrategyForResource returns the allocation strategy for a given resource
func (m *StrategyManager) getAllocationStrategyForResource(customAllocationStrategy map[string]string, resourceName string) (allocate.AllocationStrategy, error) {
	var strategyName string
	if customStrategy, exists := customAllocationStrategy[resourceName]; exists {
		strategyName = customStrategy
	} else {
		strategyName = m.GetStrategyForResource(resourceName)
	}

	return m.GetAllocationStrategy(strategyName)
}

// AllocateUsingStrategy performs allocation using the appropriate strategy for the resource
func (m *StrategyManager) AllocateUsingStrategy(ctx *allocate.AllocationContext) (*allocate.AllocationResult, error) {
	// Determine the device name
	resourceName := ctx.DeviceReq.DeviceName
	customAllocationStrategy := ctx.GPUQRMPluginConfig.CustomAllocationStrategy

	// Get the strategy for this resource
	strategy, err := m.getAllocationStrategyForResource(customAllocationStrategy, resourceName)
	if err != nil {
		return &allocate.AllocationResult{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to get strategy for resource %s: %v", resourceName, err),
		}, fmt.Errorf("failed to get strategy for resource %s: %v", resourceName, err)
	}

	general.InfoS("Using strategy for allocation",
		"resourceName", resourceName,
		"strategyName", strategy.Name(),
		"podNamespace", ctx.ResourceReq.PodNamespace,
		"podName", ctx.ResourceReq.PodName,
		"containerName", ctx.ResourceReq.ContainerName)

	// Perform allocation using the strategy
	return strategy.Allocate(ctx)
}

// RegisterGenericAllocationStrategy registers a complete generic allocation strategy with the given name
func (m *StrategyManager) RegisterGenericAllocationStrategy(
	name string, filteringNames []string, sortingName, bindingName string,
) error {
	var filteringList []allocate.FilteringStrategy
	for _, fn := range filteringNames {
		filtering, err := m.StrategyRegistry.GetFilteringStrategy(fn)
		if err != nil {
			return fmt.Errorf("filtering strategy %s not found: %v", fn, err)
		}
		filteringList = append(filteringList, filtering)
	}

	sorting, err := m.StrategyRegistry.GetSortingStrategy(sortingName)
	if err != nil {
		return fmt.Errorf("sorting strategy %s not found: %v", sortingName, err)
	}

	binding, err := m.StrategyRegistry.GetBindingStrategy(bindingName)
	if err != nil {
		return fmt.Errorf("binding strategy %s not found: %v", bindingName, err)
	}

	err = m.StrategyRegistry.RegisterAllocationStrategy(allocation.NewGenericAllocationStrategy(name, m.StrategyRegistry, filteringList, sorting, binding))
	if err != nil {
		return fmt.Errorf("register allocation strategy %s failed: %v", name, err)
	}

	general.Infof("Registered allocation strategy: %s (filtering: %s, sorting: %s, binding: %s)",
		name, filteringNames, sortingName, bindingName)
	return nil
}

// Global strategy manager instance
var (
	globalStrategyManager *StrategyManager
	once                  sync.Once
)

// GetGlobalStrategyManager returns the global strategy manager instance
func GetGlobalStrategyManager() *StrategyManager {
	once.Do(func() {
		globalStrategyManager = NewStrategyManager()

		// Register default strategies
		registerDefaultStrategies(globalStrategyManager)
	})
	return globalStrategyManager
}
