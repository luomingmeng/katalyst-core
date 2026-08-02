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

package dynamicpolicy

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	cpusetutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/util"
	"github.com/kubewharf/katalyst-core/pkg/config"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	bulkheadconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/bulkhead"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func cpuSetAdjustmentHandlerTimeout(conf *config.Configuration) time.Duration {
	if conf == nil || conf.CPUQRMPluginConfig == nil {
		return bulkheadconfig.TopologyHandlerTimeout(nil)
	}
	return bulkheadconfig.TopologyHandlerTimeout(conf.CPUQRMPluginConfig.BulkheadConfiguration)
}

func (p *DynamicPolicy) RegisterCPUSetAdjustmentHandler(name string, handler cpusetutil.CPUSetAdjustmentHandler) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("cpuset adjustment handler name is empty")
	}
	if handler == nil {
		return fmt.Errorf("cpuset adjustment handler %q is nil", name)
	}
	if p.cpuSetAdjustmentHandlers == nil {
		p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{}
	}
	if _, ok := p.cpuSetAdjustmentHandlers[name]; ok {
		return fmt.Errorf("cpuset adjustment handler %q already registered", name)
	}
	p.cpuSetAdjustmentHandlers[name] = handler
	return nil
}

type cpuSetAdjustmentStateSnapshot struct {
	machineState state.NUMANodeMap
	numaHeadroom map[int]float64
	podEntries   state.PodEntries
	allowOverlap bool
}

func newCPUSetAdjustmentStateSnapshot(source state.ReadonlyState) *cpuSetAdjustmentStateSnapshot {
	if source == nil {
		return nil
	}
	return &cpuSetAdjustmentStateSnapshot{
		machineState: source.GetMachineState(),
		numaHeadroom: source.GetNUMAHeadroom(),
		podEntries:   source.GetPodEntries(),
		allowOverlap: source.GetAllowSharedCoresOverlapReclaimedCores(),
	}
}

func (s *cpuSetAdjustmentStateSnapshot) matches(source state.ReadonlyState) bool {
	if s == nil || source == nil {
		return s == nil && source == nil
	}
	return s.allowOverlap == source.GetAllowSharedCoresOverlapReclaimedCores() &&
		reflect.DeepEqual(s.machineState, source.GetMachineState()) &&
		reflect.DeepEqual(s.numaHeadroom, source.GetNUMAHeadroom()) &&
		reflect.DeepEqual(s.podEntries, source.GetPodEntries())
}

func (s *cpuSetAdjustmentStateSnapshot) GetMachineState() state.NUMANodeMap {
	return s.machineState.Clone()
}

func (s *cpuSetAdjustmentStateSnapshot) GetNUMAHeadroom() map[int]float64 {
	out := make(map[int]float64, len(s.numaHeadroom))
	for numaID, headroom := range s.numaHeadroom {
		out[numaID] = headroom
	}
	return out
}

func (s *cpuSetAdjustmentStateSnapshot) GetPodEntries() state.PodEntries {
	return s.podEntries.Clone()
}

func (s *cpuSetAdjustmentStateSnapshot) GetAllocationInfo(podUID, containerName string) *state.AllocationInfo {
	if allocationInfo := s.podEntries[podUID][containerName]; allocationInfo != nil {
		return allocationInfo.Clone()
	}
	return nil
}

func (s *cpuSetAdjustmentStateSnapshot) GetAllowSharedCoresOverlapReclaimedCores() bool {
	return s.allowOverlap
}

func (p *DynamicPolicy) runCPUSetAdjustmentHandlers(ctx context.Context) error {
	if len(p.cpuSetAdjustmentHandlers) == 0 {
		return nil
	}

	// Serialize complete adjustment rounds without retaining the policy lock.
	// Waiting before taking the immutable snapshot ensures a queued round plans
	// from state left by the preceding round and its caller-side error handling.
	p.Unlock()
	p.cpuSetAdjustmentExecutionMu.Lock()
	p.Lock()
	defer p.cpuSetAdjustmentExecutionMu.Unlock()

	for {
		var topology *machine.CPUTopology
		if p.machineInfo != nil {
			topology = p.machineInfo.CPUTopology
		}
		var dynamicConf *dynamicconfig.Configuration
		if p.dynamicConfig != nil {
			dynamicConf = p.dynamicConfig.GetDynamicConfiguration()
		}
		stateSnapshot := newCPUSetAdjustmentStateSnapshot(p.state)
		handlerCtx := cpusetutil.CPUSetAdjustmentHandlerCtx{
			CoreConf:    p.conf,
			DynamicConf: dynamicConf,
			Emitter:     p.emitter,
			MetaServer:  p.metaServer,
			State:       stateSnapshot,
			Topology:    topology,
		}
		p.cpuSetAdjustmentGeneration++
		handlerCtx.Generation = p.cpuSetAdjustmentGeneration
		roundInvalidated := false
		handlerCtx.CommitIfGenerationCurrent = func(generation uint64, commit func()) bool {
			p.Lock()
			defer p.Unlock()
			var currentDynamicConf *dynamicconfig.Configuration
			if p.dynamicConfig != nil {
				currentDynamicConf = p.dynamicConfig.GetDynamicConfiguration()
			}
			if generation != p.cpuSetAdjustmentGeneration ||
				dynamicConf != currentDynamicConf ||
				!stateSnapshot.matches(p.state) {
				roundInvalidated = true
				return false
			}
			commit()
			return true
		}

		names := make([]string, 0, len(p.cpuSetAdjustmentHandlers))
		handlers := make(map[string]cpusetutil.CPUSetAdjustmentHandler, len(p.cpuSetAdjustmentHandlers))
		for name := range p.cpuSetAdjustmentHandlers {
			names = append(names, name)
			handlers[name] = p.cpuSetAdjustmentHandlers[name]
		}
		sort.Strings(names)

		p.Unlock()
		var roundErr error
		for _, name := range names {
			if err := handlers[name](ctx, handlerCtx); err != nil {
				roundErr = fmt.Errorf("run cpuset adjustment handler %q: %w", name, err)
				break
			}
		}
		p.Lock()
		if roundInvalidated && ctx.Err() == nil {
			continue
		}
		return roundErr
	}
}
