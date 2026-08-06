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

	cpuconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	cpusetutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/util"
	"github.com/kubewharf/katalyst-core/pkg/config"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	bulkheadconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/bulkhead"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

const (
	cpuSetAdjustmentRetryMaxAttempts    = 4
	cpuSetAdjustmentRetryInitialBackoff = 10 * time.Millisecond
	cpuSetAdjustmentRetryMaxBackoff     = 200 * time.Millisecond
)

type cpuSetAdjustmentRevisionedState interface {
	GetRevision() uint64
}

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
	machineState     state.NUMANodeMap
	numaHeadroom     map[int]float64
	podEntries       state.PodEntries
	allowOverlap     bool
	disableDedicated bool
	revision         uint64
	hasRevision      bool
}

func newCPUSetAdjustmentStateSnapshot(source state.ReadonlyState) *cpuSetAdjustmentStateSnapshot {
	if source == nil {
		return nil
	}
	snapshot := &cpuSetAdjustmentStateSnapshot{
		machineState:     source.GetMachineState(),
		numaHeadroom:     source.GetNUMAHeadroom(),
		podEntries:       source.GetPodEntries(),
		allowOverlap:     source.GetAllowSharedCoresOverlapReclaimedCores(),
		disableDedicated: source.GetDisableDedicatedCoresOverlapReclaimedCores(),
	}
	if revisioned, ok := source.(cpuSetAdjustmentRevisionedState); ok {
		snapshot.revision = revisioned.GetRevision()
		snapshot.hasRevision = true
	}
	return snapshot
}

func (s *cpuSetAdjustmentStateSnapshot) matches(source state.ReadonlyState) bool {
	if s == nil || source == nil {
		return s == nil && source == nil
	}
	if s.hasRevision {
		revisioned, ok := source.(cpuSetAdjustmentRevisionedState)
		return ok && s.revision == revisioned.GetRevision()
	}
	return s.allowOverlap == source.GetAllowSharedCoresOverlapReclaimedCores() &&
		s.disableDedicated == source.GetDisableDedicatedCoresOverlapReclaimedCores() &&
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

func (s *cpuSetAdjustmentStateSnapshot) GetDisableDedicatedCoresOverlapReclaimedCores() bool {
	return s.disableDedicated
}

func (p *DynamicPolicy) runCPUSetAdjustmentHandlers(ctx context.Context, modes ...cpusetutil.CPUSetAdjustmentMode) error {
	if len(p.cpuSetAdjustmentHandlers) == 0 {
		return nil
	}
	mode := cpusetutil.CPUSetAdjustmentModePeriodic
	if len(modes) > 0 {
		mode = modes[0].OrFullDefault()
	}

	// Serialize complete adjustment rounds without retaining the policy lock.
	// Waiting before taking the immutable snapshot ensures a queued round plans
	// from state left by the preceding round and its caller-side error handling.
	if p.cpuSetAdjustmentExecution == nil {
		p.cpuSetAdjustmentExecution = make(chan struct{}, 1)
	}
	execution := p.cpuSetAdjustmentExecution
	p.Unlock()
	select {
	case execution <- struct{}{}:
	case <-ctx.Done():
		p.Lock()
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-execution
		p.Lock()
		return err
	}
	p.Lock()
	defer func() { <-execution }()

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
		commitOverride := &cpusetutil.CPUSetAdjustmentCommitOverride{}
		handlerCtx := cpusetutil.CPUSetAdjustmentHandlerCtx{
			CoreConf:    p.conf,
			DynamicConf: dynamicConf,
			Emitter:     p.emitter,
			MetaServer:  p.metaServer,
			State:       stateSnapshot,
			Topology:    topology,
			Mode:        mode,
			ScheduleFullRetry: func(reason cpusetutil.CPUSetAdjustmentRetryReason) {
				p.scheduleCPUSetAdjustmentRetry(reason)
			},
			CommitOverride: commitOverride,
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
		if roundInvalidated {
			if ctx.Err() == nil {
				continue
			}
			p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonStaleState)
			if roundErr != nil {
				return fmt.Errorf("%v; scheduled latest cpuset adjustment after canceled stale round: %w", roundErr, ctx.Err())
			}
			return ctx.Err()
		}
		if roundErr == nil && !commitOverride.ReclaimEffective.IsEmpty() {
			newEntries := p.state.GetPodEntries()
			if err := p.syncReclaimPoolWithAdjustmentCommitOverride(newEntries, commitOverride); err != nil {
				roundErr = fmt.Errorf("sync reclaim pool from cpuset adjustment override: %w", err)
			} else {
				newMachineState, err := generateMachineStateFromPodEntries(
					p.machineInfo.CPUTopology, newEntries, p.state.GetMachineState())
				if err != nil {
					roundErr = fmt.Errorf("generate machine state from cpuset adjustment override: %w", err)
				} else if err := p.state.CommitAdvisorState(
					newEntries,
					newMachineState,
					p.state.GetAllowSharedCoresOverlapReclaimedCores(),
					p.state.GetDisableDedicatedCoresOverlapReclaimedCores(),
					true,
				); err != nil {
					roundErr = fmt.Errorf("commit cpuset adjustment override: %w", err)
				}
			}
		}
		if roundErr == nil && mode == cpusetutil.CPUSetAdjustmentModePeriodic {
			p.cpuSetAdjustmentRetryMu.Lock()
			if !p.cpuSetAdjustmentRetryQueued && !p.cpuSetAdjustmentRetryAgain {
				p.cpuSetAdjustmentRetryDirty = false
				p.cpuSetAdjustmentRetryReasons = nil
			}
			p.cpuSetAdjustmentRetryMu.Unlock()
		}
		return roundErr
	}
}

func (p *DynamicPolicy) scheduleCPUSetAdjustmentRetry(reason cpusetutil.CPUSetAdjustmentRetryReason) {
	p.cpuSetAdjustmentRetryMu.Lock()
	if p.cpuSetAdjustmentRetryStopping {
		p.cpuSetAdjustmentRetryMu.Unlock()
		return
	}
	p.cpuSetAdjustmentRetryDirty = true
	if p.cpuSetAdjustmentRetryReasons == nil {
		p.cpuSetAdjustmentRetryReasons = make(map[cpusetutil.CPUSetAdjustmentRetryReason]struct{})
	}
	p.cpuSetAdjustmentRetryReasons[reason] = struct{}{}
	if p.cpuSetAdjustmentRetryQueued {
		p.cpuSetAdjustmentRetryAgain = true
		p.cpuSetAdjustmentRetryMu.Unlock()
		return
	}
	p.cpuSetAdjustmentRetryQueued = true
	stopCh := p.cpuSetAdjustmentRetryStopCh
	p.cpuSetAdjustmentRetryWG.Add(1)
	p.cpuSetAdjustmentRetryMu.Unlock()
	go func() {
		defer p.cpuSetAdjustmentRetryWG.Done()
		finishStopped := func() {
			p.cpuSetAdjustmentRetryMu.Lock()
			p.cpuSetAdjustmentRetryQueued = false
			p.cpuSetAdjustmentRetryAgain = false
			p.cpuSetAdjustmentRetryMu.Unlock()
		}
		attempt := 0
		for {
			select {
			case <-stopCh:
				finishStopped()
				return
			default:
			}
			p.Lock()
			ctx, cancel := context.WithTimeout(context.Background(), cpuSetAdjustmentHandlerTimeout(p.conf))
			if stopCh != nil {
				go func() {
					select {
					case <-stopCh:
						cancel()
					case <-ctx.Done():
					}
				}()
			}
			err := p.runCPUSetAdjustmentHandlers(ctx, cpusetutil.CPUSetAdjustmentModeRetry)
			cancel()
			p.Unlock()
			if err != nil {
				attempt++
				general.Errorf("retry latest cpuset adjustment failed, reason=%s: %v", reason, err)
				if attempt < cpuSetAdjustmentRetryMaxAttempts {
					timer := time.NewTimer(cpuSetAdjustmentRetryBackoff(attempt))
					select {
					case <-timer.C:
					case <-stopCh:
						if !timer.Stop() {
							<-timer.C
						}
						finishStopped()
						return
					}
					continue
				}
			}

			p.cpuSetAdjustmentRetryMu.Lock()
			if p.cpuSetAdjustmentRetryStopping {
				p.cpuSetAdjustmentRetryQueued = false
				p.cpuSetAdjustmentRetryAgain = false
				p.cpuSetAdjustmentRetryMu.Unlock()
				return
			}
			if p.cpuSetAdjustmentRetryAgain {
				p.cpuSetAdjustmentRetryAgain = false
				p.cpuSetAdjustmentRetryMu.Unlock()
				attempt = 0
				continue
			}
			if err == nil {
				p.cpuSetAdjustmentRetryDirty = false
				p.cpuSetAdjustmentRetryReasons = nil
			} else {
				p.cpuSetAdjustmentRetryDirty = true
			}
			p.cpuSetAdjustmentRetryQueued = false
			p.cpuSetAdjustmentRetryMu.Unlock()
			return
		}
	}()
}

func (p *DynamicPolicy) handleCgroupCreateEvent() {
	p.cpuSetAdjustmentRetryMu.Lock()
	_, deferredLeaf := p.cpuSetAdjustmentRetryReasons[cpusetutil.RetryReasonDeferredLeaf]
	shouldRetry := p.cpuSetAdjustmentRetryDirty && deferredLeaf
	p.cpuSetAdjustmentRetryMu.Unlock()
	if shouldRetry {
		p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonDeferredLeaf)
	}
}

func (p *DynamicPolicy) reconcileDirtyCPUSetAdjustment() error {
	p.cpuSetAdjustmentRetryMu.Lock()
	dirty := p.cpuSetAdjustmentRetryDirty && !p.cpuSetAdjustmentRetryQueued
	p.cpuSetAdjustmentRetryMu.Unlock()
	if !dirty {
		return nil
	}

	p.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), cpuSetAdjustmentHandlerTimeout(p.conf))
	err := p.runCPUSetAdjustmentHandlers(ctx, cpusetutil.CPUSetAdjustmentModePeriodic)
	cancel()
	p.Unlock()
	if err != nil {
		general.Errorf("periodic latest-state cpuset adjustment reconcile failed: %v", err)
	}
	return err
}

func (p *DynamicPolicy) runBulkheadPeriodicalHandlers(
	coreConf *config.Configuration,
	extraConf interface{},
	dynamicConf *dynamicconfig.DynamicAgentConfiguration,
	emitter metrics.MetricEmitter,
	metaServer *metaserver.MetaServer,
) {
	reconcileErr := p.reconcileDirtyCPUSetAdjustment()
	if p.bulkheadManager != nil {
		p.bulkheadManager.RunPeriodicalHandlers(coreConf, extraConf, dynamicConf, emitter, metaServer)
	}
	if reconcileErr != nil {
		_ = general.UpdateHealthzStateByError(cpuconsts.SyncBulkhead, reconcileErr)
	}
}

func cpuSetAdjustmentRetryBackoff(failedAttempts int) time.Duration {
	if failedAttempts <= 1 {
		return cpuSetAdjustmentRetryInitialBackoff
	}
	backoff := cpuSetAdjustmentRetryInitialBackoff
	for i := 1; i < failedAttempts && backoff < cpuSetAdjustmentRetryMaxBackoff; i++ {
		backoff *= 2
	}
	if backoff > cpuSetAdjustmentRetryMaxBackoff {
		return cpuSetAdjustmentRetryMaxBackoff
	}
	return backoff
}
