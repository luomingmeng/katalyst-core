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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"

	cpuconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/consts"
	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
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
	advisorPostCommitCheckpointName     = "cpu_advisor_post_commit_target"
)

type cpuSetAdjustmentRevisionedState interface {
	GetRevision() uint64
}

type advisorPostCommitTarget struct {
	revision uint64
	response *advisorapi.ListAndWatchResponse
}

type advisorPostCommitCheckpoint struct {
	Revision uint64 `json:"revision"`
	Response []byte `json:"response"`
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
	defaultShare     state.DefaultShareMaterializationState
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
		defaultShare:     source.GetDefaultShareMaterializationState(),
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
		s.defaultShare == source.GetDefaultShareMaterializationState() &&
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

func (s *cpuSetAdjustmentStateSnapshot) GetDefaultShareMaterializationState() state.DefaultShareMaterializationState {
	return s.defaultShare
}

func (s *cpuSetAdjustmentStateSnapshot) GetRevision() uint64 {
	return s.revision
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
		stateRevision := uint64(0)
		if p.state != nil {
			stateRevision = p.state.GetRevision()
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
				} else if err := p.state.CommitAdvisorStateIfRevision(
					stateRevision,
					newEntries,
					newMachineState,
					p.state.GetAllowSharedCoresOverlapReclaimedCores(),
					p.state.GetDisableDedicatedCoresOverlapReclaimedCores(),
					true,
					p.state.GetDefaultShareMaterializationState(),
				); err != nil {
					if errors.Is(err, state.ErrStaleStateRevision) {
						p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonStaleState)
					}
					roundErr = fmt.Errorf("commit cpuset adjustment override: %w", err)
				}
			}
		}
		if roundErr == nil && mode == cpusetutil.CPUSetAdjustmentModePeriodic &&
			!p.hasAnyPendingAdvisorPostCommitTarget() {
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

func (p *DynamicPolicy) publishAdvisorPostCommitTarget(
	resp *advisorapi.ListAndWatchResponse,
	revision uint64,
) *advisorPostCommitTarget {
	target := cloneAdvisorPostCommitTarget(resp, revision)
	if err := p.storeAdvisorPostCommitTarget(target, p.advisorPostCommitCheckpointPath()); err != nil {
		general.Errorf("persist advisor post-commit target for revision %d failed: %v", revision, err)
	}
	p.publishPreparedAdvisorPostCommitTarget(target)
	return target
}

func cloneAdvisorPostCommitTarget(
	resp *advisorapi.ListAndWatchResponse,
	revision uint64,
) *advisorPostCommitTarget {
	cloned := &advisorapi.ListAndWatchResponse{}
	if resp != nil {
		cloned = proto.Clone(resp).(*advisorapi.ListAndWatchResponse)
	}
	return &advisorPostCommitTarget{revision: revision, response: cloned}
}

func nextAdvisorRevision(revision uint64) (uint64, error) {
	if revision == math.MaxUint64 {
		return 0, fmt.Errorf("state revision overflow at %d", revision)
	}
	return revision + 1, nil
}

func (p *DynamicPolicy) prepareAdvisorPostCommitTarget(
	resp *advisorapi.ListAndWatchResponse,
	postCommitRevision uint64,
) (*advisorPostCommitTarget, error) {
	target := cloneAdvisorPostCommitTarget(resp, postCommitRevision)
	if err := p.storeAdvisorPostCommitTarget(target, p.advisorPostCommitStagingPath()); err != nil {
		return nil, err
	}
	return target, nil
}

func (p *DynamicPolicy) publishPreparedAdvisorPostCommitTarget(target *advisorPostCommitTarget) {
	p.cpuSetAdjustmentRetryMu.Lock()
	p.advisorPostCommitTarget = target
	p.cpuSetAdjustmentRetryMu.Unlock()
}

func (p *DynamicPolicy) commitAdvisorResponseWithWriteAhead(
	resp *advisorapi.ListAndWatchResponse,
	preCommitRevision uint64,
	commitDesired func() error,
) (*advisorPostCommitTarget, error) {
	postCommitRevision, err := nextAdvisorRevision(preCommitRevision)
	if err != nil {
		return nil, err
	}
	target, err := p.prepareAdvisorPostCommitTarget(resp, postCommitRevision)
	if err != nil {
		return nil, fmt.Errorf("persist advisor post-commit target: %w", err)
	}
	if err := commitDesired(); err != nil {
		if removeErr := p.removeAdvisorPostCommitStaging(); removeErr != nil {
			return nil, fmt.Errorf("%w; remove uncommitted advisor target: %v", err, removeErr)
		}
		return nil, err
	}
	if p.state == nil || p.state.GetRevision() != postCommitRevision {
		actualRevision := uint64(0)
		if p.state != nil {
			actualRevision = p.state.GetRevision()
		}
		if removeErr := p.removeAdvisorPostCommitStaging(); removeErr != nil {
			return nil, fmt.Errorf("advisor desired commit revision mismatch: expected=%d actual=%d; remove target: %v",
				postCommitRevision, actualRevision, removeErr)
		}
		return nil, fmt.Errorf("advisor desired commit revision mismatch: expected=%d actual=%d",
			postCommitRevision, actualRevision)
	}
	if err := p.promoteAdvisorPostCommitStaging(); err != nil {
		return nil, fmt.Errorf("promote advisor post-commit target: %w", err)
	}
	p.publishPreparedAdvisorPostCommitTarget(target)
	return target, nil
}

func (p *DynamicPolicy) advisorPostCommitCheckpointPath() string {
	if p.advisorPostCommitCheckpointDir == "" {
		return ""
	}
	return filepath.Join(p.advisorPostCommitCheckpointDir, advisorPostCommitCheckpointName)
}

func (p *DynamicPolicy) advisorPostCommitStagingPath() string {
	path := p.advisorPostCommitCheckpointPath()
	if path == "" {
		return ""
	}
	return path + ".staging"
}

func (p *DynamicPolicy) storeAdvisorPostCommitTarget(target *advisorPostCommitTarget, path string) error {
	if path == "" || target == nil {
		return nil
	}
	response, err := proto.Marshal(target.response)
	if err != nil {
		return fmt.Errorf("marshal advisor response: %w", err)
	}
	data, err := json.Marshal(advisorPostCommitCheckpoint{
		Revision: target.revision,
		Response: response,
	})
	if err != nil {
		return fmt.Errorf("marshal advisor checkpoint: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create advisor checkpoint directory: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+advisorPostCommitCheckpointName+"-*")
	if err != nil {
		return fmt.Errorf("create temporary advisor checkpoint: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return fmt.Errorf("chmod temporary advisor checkpoint: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("write temporary advisor checkpoint: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temporary advisor checkpoint: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary advisor checkpoint: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("publish advisor checkpoint: %w", err)
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open advisor checkpoint directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync advisor checkpoint directory: %w", err)
	}
	return nil
}

func syncAdvisorPostCommitDirectory(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open advisor checkpoint directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync advisor checkpoint directory: %w", err)
	}
	return nil
}

func removeAdvisorPostCommitPath(path string) error {
	if path == "" {
		return nil
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove advisor checkpoint: %w", err)
	}
	return syncAdvisorPostCommitDirectory(path)
}

func (p *DynamicPolicy) removeAdvisorPostCommitCheckpoint() error {
	return removeAdvisorPostCommitPath(p.advisorPostCommitCheckpointPath())
}

func (p *DynamicPolicy) removeAdvisorPostCommitStaging() error {
	return removeAdvisorPostCommitPath(p.advisorPostCommitStagingPath())
}

func (p *DynamicPolicy) promoteAdvisorPostCommitStaging() error {
	stagingPath := p.advisorPostCommitStagingPath()
	activePath := p.advisorPostCommitCheckpointPath()
	if stagingPath == "" {
		return nil
	}
	if err := os.Rename(stagingPath, activePath); err != nil {
		return fmt.Errorf("rename staging checkpoint: %w", err)
	}
	return syncAdvisorPostCommitDirectory(activePath)
}

func loadAdvisorPostCommitTarget(path string) (*advisorPostCommitTarget, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var checkpoint advisorPostCommitCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, err
	}
	response := &advisorapi.ListAndWatchResponse{}
	if err := proto.Unmarshal(checkpoint.Response, response); err != nil {
		return nil, err
	}
	return &advisorPostCommitTarget{revision: checkpoint.Revision, response: response}, nil
}

func (p *DynamicPolicy) restoreAdvisorPostCommitTarget() error {
	activePath := p.advisorPostCommitCheckpointPath()
	if activePath == "" {
		return nil
	}
	stagingPath := p.advisorPostCommitStagingPath()
	mainRevision := uint64(0)
	if p.state != nil {
		mainRevision = p.state.GetRevision()
	}
	active, activeErr := loadAdvisorPostCommitTarget(activePath)
	staging, stagingErr := loadAdvisorPostCommitTarget(stagingPath)
	if activeErr != nil && !os.IsNotExist(activeErr) {
		general.Errorf("discard corrupted active advisor post-commit checkpoint: %v", activeErr)
	}
	if stagingErr != nil && !os.IsNotExist(stagingErr) {
		general.Errorf("discard corrupted staging advisor post-commit checkpoint: %v", stagingErr)
	}

	var selected *advisorPostCommitTarget
	if stagingErr == nil && staging.revision == mainRevision {
		selected = staging
		if err := p.promoteAdvisorPostCommitStaging(); err != nil {
			return err
		}
	} else if activeErr == nil && active.revision == mainRevision {
		selected = active
		if err := p.removeAdvisorPostCommitStaging(); err != nil {
			return err
		}
	} else {
		if err := p.removeAdvisorPostCommitCheckpoint(); err != nil {
			return err
		}
		if err := p.removeAdvisorPostCommitStaging(); err != nil {
			return err
		}
		return nil
	}
	p.cpuSetAdjustmentRetryMu.Lock()
	p.advisorPostCommitTarget = selected
	p.cpuSetAdjustmentRetryMu.Unlock()
	return nil
}

func (p *DynamicPolicy) prepareAdvisorPostCommitTargetOnStart() error {
	p.cpuSetAdjustmentRetryMu.Lock()
	current := p.advisorPostCommitTarget
	p.cpuSetAdjustmentRetryMu.Unlock()
	if current == nil {
		if err := p.restoreAdvisorPostCommitTarget(); err != nil {
			return err
		}
	}

	p.cpuSetAdjustmentRetryMu.Lock()
	defer p.cpuSetAdjustmentRetryMu.Unlock()
	if p.advisorPostCommitTarget == nil {
		return nil
	}
	if p.state == nil || p.advisorPostCommitTarget.revision != p.state.GetRevision() {
		p.advisorPostCommitTarget = nil
		return p.removeAdvisorPostCommitCheckpoint()
	}
	p.cpuSetAdjustmentRetryDirty = true
	if p.cpuSetAdjustmentRetryReasons == nil {
		p.cpuSetAdjustmentRetryReasons = make(map[cpusetutil.CPUSetAdjustmentRetryReason]struct{})
	}
	p.cpuSetAdjustmentRetryReasons[cpusetutil.RetryReasonApplyFailed] = struct{}{}
	return nil
}

func (p *DynamicPolicy) hasAnyPendingAdvisorPostCommitTarget() bool {
	p.cpuSetAdjustmentRetryMu.Lock()
	defer p.cpuSetAdjustmentRetryMu.Unlock()
	return p.advisorPostCommitTarget != nil
}

func (p *DynamicPolicy) hasPendingAdvisorPostCommitTarget(revision uint64) bool {
	p.cpuSetAdjustmentRetryMu.Lock()
	defer p.cpuSetAdjustmentRetryMu.Unlock()
	return p.advisorPostCommitTarget != nil && p.advisorPostCommitTarget.revision == revision
}

func (p *DynamicPolicy) currentAdvisorPostCommitTarget() *advisorPostCommitTarget {
	p.cpuSetAdjustmentRetryMu.Lock()
	defer p.cpuSetAdjustmentRetryMu.Unlock()
	return p.advisorPostCommitTarget
}

func (p *DynamicPolicy) reconcileAdvisorPostCommitTarget(
	ctx context.Context,
	target *advisorPostCommitTarget,
	modes ...cpusetutil.CPUSetAdjustmentMode,
) error {
	if target == nil {
		return nil
	}
	if !p.advisorPostCommitTargetCurrent(target) {
		return nil
	}

	mode := cpusetutil.CPUSetAdjustmentModePeriodic
	if len(modes) > 0 {
		mode = modes[0].OrFullDefault()
	}
	headroomErr := p.applyHeadroom(target.response)
	if !p.advisorPostCommitTargetCurrent(target) {
		return nil
	}
	cgroupErr := p.applyCgroupConfigs(target.response)
	if !p.advisorPostCommitTargetCurrent(target) {
		return nil
	}
	adjustmentErr := p.runCPUSetAdjustmentHandlers(ctx, mode)
	if headroomErr == nil && cgroupErr == nil && adjustmentErr == nil {
		p.cpuSetAdjustmentRetryMu.Lock()
		if p.advisorPostCommitTarget == target {
			if err := p.removeAdvisorPostCommitCheckpoint(); err != nil {
				p.cpuSetAdjustmentRetryMu.Unlock()
				return err
			}
			p.advisorPostCommitTarget = nil
			delete(p.cpuSetAdjustmentRetryReasons, cpusetutil.RetryReasonApplyFailed)
			if len(p.cpuSetAdjustmentRetryReasons) == 0 {
				p.cpuSetAdjustmentRetryDirty = false
				p.cpuSetAdjustmentRetryReasons = nil
			}
		}
		p.cpuSetAdjustmentRetryMu.Unlock()
		return nil
	}

	if mode != cpusetutil.CPUSetAdjustmentModeRetry {
		p.markAdvisorApplyFailed(target.revision)
	}
	var stageErrors []string
	if headroomErr != nil {
		stageErrors = append(stageErrors, fmt.Sprintf("applyHeadroom failed with error: %v", headroomErr))
	}
	if cgroupErr != nil {
		stageErrors = append(stageErrors, fmt.Sprintf("applyCgroupConfigs failed with error: %v", cgroupErr))
	}
	if adjustmentErr != nil {
		stageErrors = append(stageErrors, fmt.Sprintf("runCPUSetAdjustmentHandlers failed with error: %v", adjustmentErr))
	}
	return errors.New(strings.Join(stageErrors, "; "))
}

func (p *DynamicPolicy) advisorPostCommitTargetCurrent(target *advisorPostCommitTarget) bool {
	p.cpuSetAdjustmentRetryMu.Lock()
	defer p.cpuSetAdjustmentRetryMu.Unlock()
	if p.advisorPostCommitTarget != target {
		return false
	}
	if p.state != nil && p.state.GetRevision() == target.revision {
		return true
	}
	p.advisorPostCommitTarget = nil
	if err := p.removeAdvisorPostCommitCheckpoint(); err != nil {
		general.Errorf("remove stale advisor post-commit checkpoint failed: %v", err)
	}
	return false
}

func (p *DynamicPolicy) markAdvisorApplyFailed(revision uint64) {
	general.Errorf("post-advisor-commit apply failed for state revision %d; scheduling latest-state retry", revision)
	p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonApplyFailed)
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
			var err error
			if target := p.currentAdvisorPostCommitTarget(); target != nil {
				err = p.reconcileAdvisorPostCommitTarget(ctx, target, cpusetutil.CPUSetAdjustmentModeRetry)
			} else {
				err = p.runCPUSetAdjustmentHandlers(ctx, cpusetutil.CPUSetAdjustmentModeRetry)
			}
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
			if err == nil && p.advisorPostCommitTarget == nil {
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
	var err error
	if target := p.currentAdvisorPostCommitTarget(); target != nil {
		err = p.reconcileAdvisorPostCommitTarget(ctx, target, cpusetutil.CPUSetAdjustmentModePeriodic)
	} else {
		err = p.runCPUSetAdjustmentHandlers(ctx, cpusetutil.CPUSetAdjustmentModePeriodic)
	}
	cancel()
	p.Unlock()
	if err != nil {
		general.Errorf("periodic latest-state cpuset adjustment reconcile failed: %v", err)
	} else {
		p.cpuSetAdjustmentRetryMu.Lock()
		if p.advisorPostCommitTarget == nil && !p.cpuSetAdjustmentRetryQueued && !p.cpuSetAdjustmentRetryAgain {
			p.cpuSetAdjustmentRetryDirty = false
			p.cpuSetAdjustmentRetryReasons = nil
		}
		p.cpuSetAdjustmentRetryMu.Unlock()
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
