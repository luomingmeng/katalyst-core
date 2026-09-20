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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
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
	cpuSetAdjustmentAdmissionReplans    = 4
	advisorPostCommitCheckpointName     = "cpu_advisor_post_commit_target"
	advisorPostCommitCheckpointVersion  = 2
	advisorPostCommitWALV2Magic         = "\x00KATALYST_CPU_ADVISOR_WAL_V2\x00"
)

type advisorPostCommitPhase string

const (
	advisorPostCommitPhasePrepared      advisorPostCommitPhase = "prepared"
	advisorPostCommitPhasePublished     advisorPostCommitPhase = "published"
	advisorPostCommitPhasePhysicalApply advisorPostCommitPhase = "physical_apply"
	advisorPostCommitPhaseAppliedMarker advisorPostCommitPhase = "applied_marker"
	advisorPostCommitPhaseCleanup       advisorPostCommitPhase = "cleanup"
)

type advisorPostCommitTarget struct {
	preCommitRevision             uint64
	prepared                      bool
	abortPending                  bool
	publicationPending            bool
	applyMarkerPending            bool
	cleanupPending                bool
	previousTarget                *advisorPostCommitTarget
	applied                       bool
	checkpointVersion             int
	revision                      uint64
	response                      *advisorapi.ListAndWatchResponse
	migrationCheckpointTransition steadyFakeNUMAMigrationCheckpointTransition
	phase                         advisorPostCommitPhase
	createdAt                     time.Time
	lastProgressAt                time.Time
	progressGeneration            uint64
}

type advisorPostCommitProgress struct {
	target         *advisorPostCommitTarget
	revision       uint64
	phase          advisorPostCommitPhase
	createdAt      time.Time
	lastProgressAt time.Time
	generation     uint64
	changed        <-chan struct{}
}

type advisorPostCommitTargetContextKey struct{}

func cpuSetAdjustmentExecutionLeaseFromContext(
	ctx context.Context,
	p *DynamicPolicy,
) *cpuSetAdjustmentExecutionLease {
	lease, _ := ctx.Value(cpuSetAdjustmentExecutionLeaseContextKey{}).(*cpuSetAdjustmentExecutionLease)
	if !lease.isActiveFor(p, p.cpuSetAdjustmentExecution) {
		return nil
	}
	return lease
}

type advisorPostCommitCheckpoint struct {
	Version                       int                                      `json:"version,omitempty"`
	PreCommitRevision             *uint64                                  `json:"pre_commit_revision,omitempty"`
	Revision                      uint64                                   `json:"revision"`
	Response                      []byte                                   `json:"response"`
	MigrationCheckpointTransition *advisorMigrationCheckpointTransitionWAL `json:"migration_checkpoint_transition,omitempty"`
	Applied                       bool                                     `json:"applied,omitempty"`
	Checksum                      string                                   `json:"checksum,omitempty"`
}

type advisorMigrationCheckpointTransitionWAL struct {
	Kind             steadyFakeNUMAMigrationCheckpointTransitionKind `json:"kind"`
	ConstraintDigest string                                          `json:"constraint_digest,omitempty"`
	TargetCPUs       []int                                           `json:"target_cpus,omitempty"`
}

func advisorMigrationCheckpointTransitionToWAL(
	transition steadyFakeNUMAMigrationCheckpointTransition,
	topology *machine.CPUTopology,
) (*advisorMigrationCheckpointTransitionWAL, error) {
	switch transition.kind {
	case steadyFakeNUMAMigrationCheckpointKeep:
		return nil, nil
	case steadyFakeNUMAMigrationCheckpointRemove:
		return &advisorMigrationCheckpointTransitionWAL{
			Kind: steadyFakeNUMAMigrationCheckpointRemove,
		}, nil
	case steadyFakeNUMAMigrationCheckpointReplace:
		if transition.target == nil || transition.target.constraintDigest == "" {
			return nil, fmt.Errorf("invalid empty advisor migration checkpoint replacement")
		}
		if err := validateAdvisorMigrationCheckpointTarget(
			transition.target.target, topology); err != nil {
			return nil, err
		}
		return &advisorMigrationCheckpointTransitionWAL{
			Kind:             steadyFakeNUMAMigrationCheckpointReplace,
			ConstraintDigest: transition.target.constraintDigest,
			TargetCPUs:       transition.target.target.ToSliceInt(),
		}, nil
	default:
		return nil, fmt.Errorf(
			"invalid advisor migration checkpoint transition %d", transition.kind)
	}
}

func advisorMigrationCheckpointTransitionFromWAL(
	transition *advisorMigrationCheckpointTransitionWAL,
	topology *machine.CPUTopology,
) (steadyFakeNUMAMigrationCheckpointTransition, error) {
	keep := steadyFakeNUMAMigrationCheckpointTransition{
		kind: steadyFakeNUMAMigrationCheckpointKeep,
	}
	if transition == nil {
		return keep, nil
	}
	switch transition.Kind {
	case steadyFakeNUMAMigrationCheckpointRemove:
		return steadyFakeNUMAMigrationCheckpointTransition{
			kind: steadyFakeNUMAMigrationCheckpointRemove,
		}, nil
	case steadyFakeNUMAMigrationCheckpointReplace:
		if transition.ConstraintDigest == "" {
			return keep, fmt.Errorf(
				"advisor migration checkpoint replacement has empty constraint digest")
		}
		target := machine.NewCPUSet(transition.TargetCPUs...)
		if target.Size() != len(transition.TargetCPUs) {
			return keep, fmt.Errorf(
				"advisor migration checkpoint replacement contains duplicate CPUs")
		}
		if err := validateAdvisorMigrationCheckpointTarget(target, topology); err != nil {
			return keep, err
		}
		return steadyFakeNUMAMigrationCheckpointTransition{
			kind: steadyFakeNUMAMigrationCheckpointReplace,
			target: &steadyFakeNUMAMigrationTarget{
				constraintDigest: transition.ConstraintDigest,
				target:           target,
			},
		}, nil
	default:
		return keep, fmt.Errorf(
			"invalid advisor migration checkpoint transition %d", transition.Kind)
	}
}

func validateAdvisorMigrationCheckpointTarget(
	target machine.CPUSet,
	topology *machine.CPUTopology,
) error {
	if topology == nil {
		return fmt.Errorf("advisor migration checkpoint validation requires CPU topology")
	}
	if outside := target.Difference(topology.CPUDetails.CPUs()); !outside.IsEmpty() {
		return fmt.Errorf(
			"advisor migration checkpoint target contains CPUs outside topology: %s",
			outside.String())
	}
	if err := assertCoreAligned(target, topology); err != nil {
		return fmt.Errorf("advisor migration checkpoint target is not core aligned: %w", err)
	}
	return nil
}

func advisorPostCommitCheckpointChecksum(
	version int,
	preCommitRevision *uint64,
	revision uint64,
	response []byte,
	transition *advisorMigrationCheckpointTransitionWAL,
	applied ...bool,
) string {
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "%d\n%d\n", version, revision)
	if preCommitRevision != nil {
		_, _ = fmt.Fprintf(hash, "%d\n", *preCommitRevision)
	}
	_, _ = hash.Write(response)
	_, _ = hash.Write([]byte{'\n'})
	transitionBytes, _ := json.Marshal(transition)
	_, _ = hash.Write(transitionBytes)
	if len(applied) > 0 && applied[0] {
		_, _ = hash.Write([]byte("\napplied"))
	}
	return hex.EncodeToString(hash.Sum(nil))
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
	// revision is the sole owner of snapshot freshness; state payloads are views, not identities.
	revision uint64
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
		revision:         source.GetRevision(),
	}
	return snapshot
}

func (s *cpuSetAdjustmentStateSnapshot) matches(source state.ReadonlyState) bool {
	if s == nil || source == nil {
		return s == nil && source == nil
	}
	return s.revision == source.GetRevision()
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

func (s *cpuSetAdjustmentStateSnapshot) GetRevision() uint64 {
	return s.revision
}

func (p *DynamicPolicy) runCPUSetAdjustmentHandlers(ctx context.Context, modes ...cpusetutil.CPUSetAdjustmentMode) error {
	if len(p.cpuSetAdjustmentHandlers) == 0 {
		return nil
	}
	reconcileTarget, _ := ctx.Value(advisorPostCommitTargetContextKey{}).(*advisorPostCommitTarget)
	mode := cpusetutil.CPUSetAdjustmentModePeriodic
	if len(modes) > 0 {
		mode = modes[0].OrFullDefault()
	}

	// Serialize complete adjustment rounds without retaining the policy lock.
	// Waiting before taking the immutable snapshot ensures a queued round plans
	// from state left by the preceding round and its caller-side error handling.
	executionLease := cpuSetAdjustmentExecutionLeaseFromContext(ctx, p)
	if executionLease == nil {
		var err error
		executionLease, err = p.acquireCPUSetAdjustmentExecutionLocked(ctx)
		if err != nil {
			return err
		}
		defer executionLease.release()
		ctx = context.WithValue(ctx, cpuSetAdjustmentExecutionLeaseContextKey{}, executionLease)
	}

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
			CoreConf:                  p.conf,
			DynamicConf:               dynamicConf,
			Emitter:                   p.emitter,
			MetaServer:                p.metaServer,
			State:                     stateSnapshot,
			Topology:                  topology,
			ReservedCPUs:              p.reservedCPUs.Clone(),
			ReservedReclaimedCPUs:     p.reservedReclaimedCPUSet.Clone(),
			ReservedReclaimedCPUsSize: p.reservedReclaimedCPUsSize,
			Mode:                      mode,
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
			for attempt := 1; ; attempt++ {
				err := handlers[name](ctx, handlerCtx)
				if err == nil {
					break
				}
				wrapped := fmt.Errorf("run cpuset adjustment handler %q: %w", name, err)
				if mode == cpusetutil.CPUSetAdjustmentModeAdmission &&
					attempt < cpuSetAdjustmentAdmissionReplans &&
					isFrozenSnapshotDriftReplanSafe(err) && ctx.Err() == nil {
					general.InfoS("retry cpuset adjustment handler after safe frozen snapshot drift",
						"handler", name, "attempt", attempt)
					continue
				}
				roundErr = wrapped
				break
			}
			if roundErr != nil {
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
		reclaimOverrideTrimmed := false
		if roundErr == nil && commitOverride.Source != "" {
			appliedReclaim := commitOverride.ReclaimEffective.Clone()
			alignedReclaim, err := p.coreAlignedReclaimOverride(
				commitOverride.ReclaimEffective,
				p.state.GetDisableDedicatedCoresOverlapReclaimedCores(),
			)
			if err != nil {
				roundErr = fmt.Errorf("align reclaim cpuset adjustment override: %w", err)
			} else {
				commitOverride.ReclaimEffective = alignedReclaim
				reclaimOverrideTrimmed = !alignedReclaim.Equals(appliedReclaim)
			}
		}
		if roundErr == nil && commitOverride.Source != "" {
			newEntries := p.state.GetPodEntries()
			if err := p.syncReclaimPoolWithAdjustmentCommitOverride(newEntries, commitOverride); err != nil {
				roundErr = fmt.Errorf("sync reclaim pool from cpuset adjustment override: %w", err)
			} else {
				_, _, err := p.commitPendingCPUPartitionForAdvisorTarget(pendingCPUPartition{
					expectedRevision:          stateRevision,
					entries:                   newEntries,
					allowOverlap:              p.state.GetAllowSharedCoresOverlapReclaimedCores(),
					disableDedicated:          p.state.GetDisableDedicatedCoresOverlapReclaimedCores(),
					persist:                   true,
					source:                    "cpuset override",
					validate:                  p.validatePendingAdvisorPartitionView,
					requireCoreAlignedReclaim: p.state.GetDisableDedicatedCoresOverlapReclaimedCores(),
				}, reconcileTarget)
				if err != nil {
					if errors.Is(err, state.ErrStaleStateRevision) {
						p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonStaleState)
					}
					roundErr = fmt.Errorf("commit cpuset adjustment override: %w", err)
				} else if reclaimOverrideTrimmed {
					p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonRecoveryCommit)
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

func isFrozenSnapshotDriftReplanSafe(err error) bool {
	var initialDrift interface{ FrozenInitialSnapshotDrift() bool }
	if errors.As(err, &initialDrift) && initialDrift.FrozenInitialSnapshotDrift() {
		return true
	}
	var verifiedRollback interface{ FrozenSnapshotDriftReplanSafe() bool }
	return errors.As(err, &verifiedRollback) && verifiedRollback.FrozenSnapshotDriftReplanSafe()
}

func cloneAdvisorPostCommitTarget(
	resp *advisorapi.ListAndWatchResponse,
	revision uint64,
	transitions ...steadyFakeNUMAMigrationCheckpointTransition,
) *advisorPostCommitTarget {
	cloned := &advisorapi.ListAndWatchResponse{}
	if resp != nil {
		cloned = proto.Clone(resp).(*advisorapi.ListAndWatchResponse)
	}
	transition := steadyFakeNUMAMigrationCheckpointTransition{
		kind: steadyFakeNUMAMigrationCheckpointKeep,
	}
	if len(transitions) > 0 {
		transition = cloneSteadyFakeNUMAMigrationCheckpointTransition(transitions[0])
	}
	target := &advisorPostCommitTarget{
		checkpointVersion:             advisorPostCommitCheckpointVersion,
		revision:                      revision,
		response:                      cloned,
		migrationCheckpointTransition: transition,
	}
	initializeAdvisorPostCommitProgress(target, advisorPostCommitPhasePrepared)
	return target
}

func initializeAdvisorPostCommitProgress(target *advisorPostCommitTarget, phase advisorPostCommitPhase) {
	if target == nil {
		return
	}
	now := time.Now()
	target.phase = phase
	target.createdAt = now
	target.lastProgressAt = now
	target.progressGeneration = 1
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
	transitions ...steadyFakeNUMAMigrationCheckpointTransition,
) (*advisorPostCommitTarget, error) {
	target := cloneAdvisorPostCommitTarget(resp, postCommitRevision, transitions...)
	if postCommitRevision > 0 {
		target.preCommitRevision = postCommitRevision - 1
	}
	if err := p.storeAdvisorPostCommitTarget(target, p.advisorPostCommitStagingPath()); err != nil {
		return nil, err
	}
	return target, nil
}

func (p *DynamicPolicy) publishPreparedAdvisorPostCommitTarget(target *advisorPostCommitTarget) {
	p.cpuSetAdjustmentRetryMu.Lock()
	p.setAdvisorPostCommitTargetLocked(target)
	p.recordAdvisorPostCommitProgressLocked(target, advisorPostCommitPhasePublished)
	p.cpuSetAdjustmentRetryMu.Unlock()
}

func (p *DynamicPolicy) beginPreparedAdvisorPostCommitTarget(
	target *advisorPostCommitTarget,
	preCommitRevision uint64,
) *advisorPostCommitTarget {
	p.cpuSetAdjustmentRetryMu.Lock()
	defer p.cpuSetAdjustmentRetryMu.Unlock()
	previous := p.advisorPostCommitTarget
	target.preCommitRevision = preCommitRevision
	target.prepared = true
	p.setAdvisorPostCommitTargetLocked(target)
	p.recordAdvisorPostCommitProgressLocked(target, advisorPostCommitPhasePrepared)
	return previous
}

func (p *DynamicPolicy) finishPreparedAdvisorPostCommitTarget(target *advisorPostCommitTarget) {
	p.cpuSetAdjustmentRetryMu.Lock()
	defer p.cpuSetAdjustmentRetryMu.Unlock()
	if p.advisorPostCommitTarget == target {
		target.prepared = false
	}
}

func (p *DynamicPolicy) rollbackPreparedAdvisorPostCommitTarget(
	target, previous *advisorPostCommitTarget,
) {
	p.cpuSetAdjustmentRetryMu.Lock()
	defer p.cpuSetAdjustmentRetryMu.Unlock()
	if p.advisorPostCommitTarget == target {
		p.setAdvisorPostCommitTargetLocked(previous)
	}
	for permit, permitTarget := range p.advisorStateWritePermits {
		if permitTarget == target {
			delete(p.advisorStateWritePermits, permit)
		}
	}
}

func (p *DynamicPolicy) markAdvisorPostCommitAbortPending(
	target, previous *advisorPostCommitTarget,
) {
	p.cpuSetAdjustmentRetryMu.Lock()
	if p.advisorPostCommitTarget == target {
		target.abortPending = true
		target.previousTarget = previous
	}
	p.cpuSetAdjustmentRetryMu.Unlock()
	p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonApplyFailed)
}

func (p *DynamicPolicy) markAdvisorPostCommitPublicationPending(target *advisorPostCommitTarget) {
	p.cpuSetAdjustmentRetryMu.Lock()
	if p.advisorPostCommitTarget == target {
		target.publicationPending = true
	}
	p.cpuSetAdjustmentRetryMu.Unlock()
	p.markAdvisorApplyFailed(target.revision)
}

func (p *DynamicPolicy) commitAdvisorResponseWithWriteAhead(
	resp *advisorapi.ListAndWatchResponse,
	preCommitRevision uint64,
	commitDesired func(*advisorPostCommitTarget) error,
) (*advisorPostCommitTarget, error) {
	return p.commitAdvisorResponseWithWriteAheadTransition(
		resp,
		steadyFakeNUMAMigrationCheckpointTransition{
			kind: steadyFakeNUMAMigrationCheckpointKeep,
		},
		preCommitRevision,
		commitDesired,
	)
}

func (p *DynamicPolicy) commitAdvisorResponseWithWriteAheadTransition(
	resp *advisorapi.ListAndWatchResponse,
	transition steadyFakeNUMAMigrationCheckpointTransition,
	preCommitRevision uint64,
	commitDesired func(*advisorPostCommitTarget) error,
) (*advisorPostCommitTarget, error) {
	postCommitRevision, err := nextAdvisorRevision(preCommitRevision)
	if err != nil {
		return nil, err
	}
	target := cloneAdvisorPostCommitTarget(resp, postCommitRevision, transition)
	p.installCPUStateWritePermit()
	previousTarget := p.beginPreparedAdvisorPostCommitTarget(target, preCommitRevision)
	if err := p.storeAdvisorPostCommitTarget(target, p.advisorPostCommitStagingPath()); err != nil {
		if removeErr := p.removeAdvisorPostCommitStaging(); removeErr != nil {
			p.markAdvisorPostCommitAbortPending(target, previousTarget)
			return nil, fmt.Errorf(
				"persist advisor post-commit target: %w; remove incomplete target while retaining writer fence: %v",
				err, removeErr)
		}
		p.rollbackPreparedAdvisorPostCommitTarget(target, previousTarget)
		return nil, fmt.Errorf("persist advisor post-commit target: %w", err)
	}
	if err := commitDesired(target); err != nil {
		if removeErr := p.removeAdvisorPostCommitStaging(); removeErr != nil {
			p.markAdvisorPostCommitAbortPending(target, previousTarget)
			return nil, fmt.Errorf("%w; remove uncommitted advisor target: %v", err, removeErr)
		}
		p.rollbackPreparedAdvisorPostCommitTarget(target, previousTarget)
		return nil, err
	}
	if p.state == nil || p.state.GetRevision() != postCommitRevision {
		actualRevision := uint64(0)
		if p.state != nil {
			actualRevision = p.state.GetRevision()
		}
		if removeErr := p.removeAdvisorPostCommitStaging(); removeErr != nil {
			p.markAdvisorPostCommitAbortPending(target, previousTarget)
			return nil, fmt.Errorf("advisor desired commit revision mismatch: expected=%d actual=%d; remove target: %v",
				postCommitRevision, actualRevision, removeErr)
		}
		p.rollbackPreparedAdvisorPostCommitTarget(target, previousTarget)
		return nil, fmt.Errorf("advisor desired commit revision mismatch: expected=%d actual=%d",
			postCommitRevision, actualRevision)
	}
	// The prepared target already fences every unrelated writer. Once the
	// canonical revision advances, switch it to post-commit reconciliation
	// before promoting the durable staging record.
	p.finishPreparedAdvisorPostCommitTarget(target)
	if err := p.promoteAdvisorPostCommitStaging(); err != nil {
		p.markAdvisorPostCommitPublicationPending(target)
		return nil, fmt.Errorf("promote advisor post-commit target: %w", err)
	}
	p.recordAdvisorPostCommitProgress(target, advisorPostCommitPhasePublished)
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
	var topology *machine.CPUTopology
	if p.machineInfo != nil {
		topology = p.machineInfo.CPUTopology
	}
	transition, err := advisorMigrationCheckpointTransitionToWAL(
		target.migrationCheckpointTransition, topology)
	if err != nil {
		return err
	}
	checkpoint := advisorPostCommitCheckpoint{
		Version:                       advisorPostCommitCheckpointVersion,
		PreCommitRevision:             &target.preCommitRevision,
		Revision:                      target.revision,
		Response:                      append([]byte(advisorPostCommitWALV2Magic), response...),
		MigrationCheckpointTransition: transition,
		Applied:                       target.applied,
	}
	checkpoint.Checksum = advisorPostCommitCheckpointChecksum(
		checkpoint.Version, checkpoint.PreCommitRevision, checkpoint.Revision, response, transition, checkpoint.Applied)
	data, err := json.Marshal(checkpoint)
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
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) || errors.Is(err, syscall.ENOTDIR) {
			return nil
		}
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

func (p *DynamicPolicy) removeAdvisorPostCommitCheckpoints() error {
	if err := p.removeAdvisorPostCommitStaging(); err != nil {
		return err
	}
	return p.removeAdvisorPostCommitCheckpoint()
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

func advisorPostCommitTargetsEqual(left, right *advisorPostCommitTarget) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.checkpointVersion == right.checkpointVersion &&
		left.preCommitRevision == right.preCommitRevision &&
		left.revision == right.revision &&
		proto.Equal(left.response, right.response) &&
		reflect.DeepEqual(left.migrationCheckpointTransition, right.migrationCheckpointTransition)
}

func (p *DynamicPolicy) ensureAdvisorPostCommitPublished(target *advisorPostCommitTarget) error {
	stagingPath := p.advisorPostCommitStagingPath()
	if stagingPath == "" {
		return nil
	}
	if _, err := os.Stat(stagingPath); err == nil {
		return p.promoteAdvisorPostCommitStaging()
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat advisor staging checkpoint: %w", err)
	}

	activePath := p.advisorPostCommitCheckpointPath()
	var topology *machine.CPUTopology
	if p.machineInfo != nil {
		topology = p.machineInfo.CPUTopology
	}
	active, err := loadAdvisorPostCommitTarget(activePath, topology)
	if err != nil {
		return fmt.Errorf("load promoted advisor checkpoint: %w", err)
	}
	if !advisorPostCommitTargetsEqual(active, target) {
		return fmt.Errorf("promoted advisor checkpoint does not match pending revision %d", target.revision)
	}
	return syncAdvisorPostCommitDirectory(activePath)
}

func loadAdvisorPostCommitTarget(
	path string,
	topology *machine.CPUTopology,
) (*advisorPostCommitTarget, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var checkpoint advisorPostCommitCheckpoint
	if err := decoder.Decode(&checkpoint); err != nil {
		return nil, fmt.Errorf("decode advisor checkpoint: %w", err)
	}
	if err := ensureSteadyFakeNUMACheckpointEOF(decoder); err != nil {
		return nil, fmt.Errorf("advisor checkpoint trailing data: %w", err)
	}
	responseBytes := checkpoint.Response
	switch checkpoint.Version {
	case 0:
		if checkpoint.Checksum != "" || checkpoint.MigrationCheckpointTransition != nil ||
			checkpoint.Applied ||
			bytes.HasPrefix(responseBytes, []byte(advisorPostCommitWALV2Magic)) {
			return nil, fmt.Errorf("advisor checkpoint version is missing")
		}
	case advisorPostCommitCheckpointVersion:
		if checkpoint.Checksum == "" {
			return nil, fmt.Errorf("advisor checkpoint checksum is missing")
		}
		if checkpoint.PreCommitRevision == nil {
			return nil, fmt.Errorf("advisor checkpoint pre-commit revision is missing")
		}
		if !bytes.HasPrefix(responseBytes, []byte(advisorPostCommitWALV2Magic)) {
			return nil, fmt.Errorf("advisor WAL V2 magic is missing")
		}
		responseBytes = responseBytes[len(advisorPostCommitWALV2Magic):]
		if checkpoint.Checksum != advisorPostCommitCheckpointChecksum(
			checkpoint.Version,
			checkpoint.PreCommitRevision,
			checkpoint.Revision,
			responseBytes,
			checkpoint.MigrationCheckpointTransition,
			checkpoint.Applied,
		) {
			return nil, fmt.Errorf("advisor checkpoint checksum mismatch")
		}
	default:
		return nil, fmt.Errorf(
			"unsupported advisor checkpoint version %d", checkpoint.Version)
	}
	response := &advisorapi.ListAndWatchResponse{}
	if err := proto.Unmarshal(responseBytes, response); err != nil {
		return nil, err
	}
	transition, err := advisorMigrationCheckpointTransitionFromWAL(
		checkpoint.MigrationCheckpointTransition, topology)
	if err != nil {
		return nil, err
	}
	if checkpoint.Version == advisorPostCommitCheckpointVersion {
		postCommitRevision, err := nextAdvisorRevision(*checkpoint.PreCommitRevision)
		if err != nil {
			return nil, fmt.Errorf("invalid advisor checkpoint revision transition: %w", err)
		}
		if checkpoint.Revision != postCommitRevision {
			return nil, fmt.Errorf(
				"invalid advisor checkpoint revision transition: pre=%d post=%d",
				*checkpoint.PreCommitRevision, checkpoint.Revision)
		}
	}
	target := &advisorPostCommitTarget{
		checkpointVersion:             checkpoint.Version,
		revision:                      checkpoint.Revision,
		response:                      response,
		migrationCheckpointTransition: transition,
		applied:                       checkpoint.Applied,
	}
	phase := advisorPostCommitPhasePublished
	if checkpoint.Applied {
		phase = advisorPostCommitPhaseCleanup
	}
	initializeAdvisorPostCommitProgress(target, phase)
	if checkpoint.PreCommitRevision != nil {
		target.preCommitRevision = *checkpoint.PreCommitRevision
	}
	return target, nil
}

type advisorPostCommitRecoveryState int

const (
	advisorPostCommitRecoveryCleanup advisorPostCommitRecoveryState = iota
	advisorPostCommitRecoveryReplay
	advisorPostCommitRecoveryBeforeCommit
)

func advisorPostCommitRecoveryForRevision(
	target *advisorPostCommitTarget,
	currentRevision uint64,
) advisorPostCommitRecoveryState {
	if target == nil {
		return advisorPostCommitRecoveryCleanup
	}
	if target.checkpointVersion != advisorPostCommitCheckpointVersion {
		if currentRevision == target.revision {
			return advisorPostCommitRecoveryReplay
		}
		return advisorPostCommitRecoveryCleanup
	}
	if currentRevision == target.revision {
		return advisorPostCommitRecoveryReplay
	}
	if currentRevision == target.preCommitRevision {
		return advisorPostCommitRecoveryBeforeCommit
	}
	return advisorPostCommitRecoveryCleanup
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
	var topology *machine.CPUTopology
	if p.machineInfo != nil {
		topology = p.machineInfo.CPUTopology
	}
	active, activeErr := loadAdvisorPostCommitTarget(activePath, topology)
	staging, stagingErr := loadAdvisorPostCommitTarget(stagingPath, topology)

	var selected *advisorPostCommitTarget
	if stagingErr == nil &&
		advisorPostCommitRecoveryForRevision(staging, mainRevision) == advisorPostCommitRecoveryReplay {
		selected = staging
		if err := p.promoteAdvisorPostCommitStaging(); err != nil {
			return err
		}
	} else if activeErr == nil &&
		advisorPostCommitRecoveryForRevision(active, mainRevision) == advisorPostCommitRecoveryReplay {
		selected = active
		if err := p.removeAdvisorPostCommitStaging(); err != nil {
			return err
		}
	} else {
		if activeErr != nil && !os.IsNotExist(activeErr) {
			return fmt.Errorf("corrupted active advisor post-commit checkpoint: %w", activeErr)
		}
		if stagingErr != nil && !os.IsNotExist(stagingErr) {
			return fmt.Errorf("corrupted staging advisor post-commit checkpoint: %w", stagingErr)
		}
		if err := p.removeAdvisorPostCommitCheckpoint(); err != nil {
			return err
		}
		if err := p.removeAdvisorPostCommitStaging(); err != nil {
			return err
		}
		return nil
	}
	p.cpuSetAdjustmentRetryMu.Lock()
	p.setAdvisorPostCommitTargetLocked(selected)
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

	stateRevision := uint64(0)
	if p.state != nil {
		stateRevision = p.state.GetRevision()
	}

	p.cpuSetAdjustmentRetryMu.Lock()
	defer p.cpuSetAdjustmentRetryMu.Unlock()
	if p.advisorPostCommitTarget == nil {
		return nil
	}
	if p.state == nil || p.advisorPostCommitTarget.revision != stateRevision {
		p.setAdvisorPostCommitTargetLocked(nil)
		return p.removeAdvisorPostCommitCheckpoints()
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

func (p *DynamicPolicy) installCPUStateWritePermit() {
	p.state.SetWritePermit(func(revision uint64, operation string, permit *state.WritePermit) error {
		if permit == nil {
			return p.ensureCPUStateWriterAllowed(revision, "state."+operation, nil)
		}

		p.cpuSetAdjustmentRetryMu.Lock()
		defer p.cpuSetAdjustmentRetryMu.Unlock()
		target, ok := p.advisorStateWritePermits[permit]
		delete(p.advisorStateWritePermits, permit)
		if ok && target != nil && p.advisorPostCommitTarget == target {
			if target.prepared && revision == target.preCommitRevision {
				return nil
			}
			if !target.prepared && revision == target.revision {
				return nil
			}
		}
		pendingRevision := uint64(0)
		if p.advisorPostCommitTarget != nil {
			pendingRevision = p.advisorPostCommitTarget.revision
		}
		return &advisorPostCommitPendingError{
			pendingRevision:   pendingRevision,
			attemptedRevision: revision,
			source:            "state." + operation,
		}
	})
}

func (p *DynamicPolicy) newAdvisorStateWritePermit(target *advisorPostCommitTarget) *state.WritePermit {
	permit := state.NewWritePermit()
	p.cpuSetAdjustmentRetryMu.Lock()
	if p.advisorStateWritePermits == nil {
		p.advisorStateWritePermits = make(map[*state.WritePermit]*advisorPostCommitTarget)
	}
	p.advisorStateWritePermits[permit] = target
	p.cpuSetAdjustmentRetryMu.Unlock()
	return permit
}

func (p *DynamicPolicy) ensureCPUStateWriterAllowed(
	attemptedRevision uint64,
	source string,
	reconcileTarget *advisorPostCommitTarget,
) error {
	p.cpuSetAdjustmentRetryMu.Lock()
	pendingTarget := p.advisorPostCommitTarget
	if pendingTarget == nil {
		p.cpuSetAdjustmentRetryMu.Unlock()
		return nil
	}
	pendingPrepared := pendingTarget.prepared
	pendingPreCommitRevision := pendingTarget.preCommitRevision
	pendingRevision := pendingTarget.revision
	p.cpuSetAdjustmentRetryMu.Unlock()
	if reconcileTarget == pendingTarget {
		if pendingPrepared && attemptedRevision == pendingPreCommitRevision {
			return nil
		}
		if !pendingPrepared && attemptedRevision == pendingRevision {
			return nil
		}
	}
	return &advisorPostCommitPendingError{
		pendingRevision:   pendingRevision,
		attemptedRevision: attemptedRevision,
		source:            source,
	}
}

func (p *DynamicPolicy) currentAdvisorPostCommitTarget() *advisorPostCommitTarget {
	p.cpuSetAdjustmentRetryMu.Lock()
	defer p.cpuSetAdjustmentRetryMu.Unlock()
	return p.advisorPostCommitTarget
}

func (p *DynamicPolicy) currentAdvisorPostCommitTargetAndChange() (*advisorPostCommitTarget, <-chan struct{}) {
	p.cpuSetAdjustmentRetryMu.Lock()
	defer p.cpuSetAdjustmentRetryMu.Unlock()
	if p.advisorPostCommitTargetChange == nil {
		p.advisorPostCommitTargetChange = make(chan struct{})
	}
	return p.advisorPostCommitTarget, p.advisorPostCommitTargetChange
}

func (p *DynamicPolicy) currentAdvisorPostCommitProgress() advisorPostCommitProgress {
	p.cpuSetAdjustmentRetryMu.Lock()
	defer p.cpuSetAdjustmentRetryMu.Unlock()
	if p.advisorPostCommitTargetChange == nil {
		p.advisorPostCommitTargetChange = make(chan struct{})
	}
	target := p.advisorPostCommitTarget
	if target == nil {
		return advisorPostCommitProgress{changed: p.advisorPostCommitTargetChange}
	}
	return advisorPostCommitProgress{
		target:         target,
		revision:       target.revision,
		phase:          target.phase,
		createdAt:      target.createdAt,
		lastProgressAt: target.lastProgressAt,
		generation:     target.progressGeneration,
		changed:        p.advisorPostCommitTargetChange,
	}
}

func (p *DynamicPolicy) recordAdvisorPostCommitProgress(
	target *advisorPostCommitTarget,
	phase advisorPostCommitPhase,
) {
	p.cpuSetAdjustmentRetryMu.Lock()
	defer p.cpuSetAdjustmentRetryMu.Unlock()
	p.recordAdvisorPostCommitProgressLocked(target, phase)
}

// recordAdvisorPostCommitProgressLocked records runtime-only progress and wakes
// waiters. It deliberately does not alter the WAL or canonical state.
func (p *DynamicPolicy) recordAdvisorPostCommitProgressLocked(
	target *advisorPostCommitTarget,
	phase advisorPostCommitPhase,
) {
	if target == nil || p.advisorPostCommitTarget != target {
		return
	}
	if target.phase == phase && target.progressGeneration > 0 {
		return
	}
	now := time.Now()
	if target.createdAt.IsZero() {
		target.createdAt = now
	}
	target.phase = phase
	target.lastProgressAt = now
	target.progressGeneration++
	if p.advisorPostCommitTargetChange != nil {
		close(p.advisorPostCommitTargetChange)
	}
	p.advisorPostCommitTargetChange = make(chan struct{})
}

// setAdvisorPostCommitTargetLocked publishes a pointer transition and wakes all
// waiters that atomically observed the previous target and change channel.
// cpuSetAdjustmentRetryMu must be held by the caller.
func (p *DynamicPolicy) setAdvisorPostCommitTargetLocked(target *advisorPostCommitTarget) {
	if p.advisorPostCommitTarget == target {
		return
	}
	if p.advisorPostCommitTargetChange != nil {
		close(p.advisorPostCommitTargetChange)
	}
	p.advisorPostCommitTarget = target
	p.advisorPostCommitTargetChange = make(chan struct{})
}

func (p *DynamicPolicy) retryAdvisorPostCommitAbort(target *advisorPostCommitTarget) error {
	if err := p.removeAdvisorPostCommitStaging(); err != nil {
		p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonApplyFailed)
		return fmt.Errorf("remove aborted advisor post-commit staging: %w", err)
	}
	previous := target.previousTarget
	p.rollbackPreparedAdvisorPostCommitTarget(target, previous)
	p.cpuSetAdjustmentRetryMu.Lock()
	delete(p.cpuSetAdjustmentRetryReasons, cpusetutil.RetryReasonApplyFailed)
	if len(p.cpuSetAdjustmentRetryReasons) == 0 && previous == nil {
		p.cpuSetAdjustmentRetryDirty = false
		p.cpuSetAdjustmentRetryReasons = nil
	}
	p.cpuSetAdjustmentRetryMu.Unlock()
	return nil
}

func (p *DynamicPolicy) retryAdvisorPostCommitPublication(target *advisorPostCommitTarget) error {
	if err := p.ensureAdvisorPostCommitPublished(target); err != nil {
		p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonApplyFailed)
		return fmt.Errorf("publish committed advisor post-commit target: %w", err)
	}
	p.cpuSetAdjustmentRetryMu.Lock()
	if p.advisorPostCommitTarget == target {
		target.publicationPending = false
		p.recordAdvisorPostCommitProgressLocked(target, advisorPostCommitPhasePublished)
	}
	p.cpuSetAdjustmentRetryMu.Unlock()
	return nil
}

func (p *DynamicPolicy) persistAdvisorPostCommitApplied(target *advisorPostCommitTarget) error {
	if err := p.storeAdvisorPostCommitTarget(target, p.advisorPostCommitCheckpointPath()); err != nil {
		p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonApplyFailed)
		return fmt.Errorf("persist applied advisor post-commit target: %w", err)
	}
	p.cpuSetAdjustmentRetryMu.Lock()
	if p.advisorPostCommitTarget == target {
		target.applyMarkerPending = false
		target.cleanupPending = true
		p.recordAdvisorPostCommitProgressLocked(target, advisorPostCommitPhaseCleanup)
	}
	p.cpuSetAdjustmentRetryMu.Unlock()
	return nil
}

func (p *DynamicPolicy) completeAdvisorPostCommitCleanup(target *advisorPostCommitTarget) error {
	if err := p.removeAdvisorPostCommitCheckpoints(); err != nil {
		p.cpuSetAdjustmentRetryMu.Lock()
		if p.advisorPostCommitTarget == target {
			target.cleanupPending = true
		}
		p.cpuSetAdjustmentRetryMu.Unlock()
		p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonApplyFailed)
		return fmt.Errorf("remove reconciled advisor post-commit checkpoints: %w", err)
	}

	p.cpuSetAdjustmentRetryMu.Lock()
	if p.advisorPostCommitTarget == target {
		p.setAdvisorPostCommitTargetLocked(nil)
		for permit, permitTarget := range p.advisorStateWritePermits {
			if permitTarget == target {
				delete(p.advisorStateWritePermits, permit)
			}
		}
		delete(p.cpuSetAdjustmentRetryReasons, cpusetutil.RetryReasonApplyFailed)
		if len(p.cpuSetAdjustmentRetryReasons) == 0 {
			p.cpuSetAdjustmentRetryDirty = false
			p.cpuSetAdjustmentRetryReasons = nil
		}
	}
	p.cpuSetAdjustmentRetryMu.Unlock()
	return nil
}

func (p *DynamicPolicy) reconcileAdvisorPostCommitTarget(
	ctx context.Context,
	target *advisorPostCommitTarget,
	modes ...cpusetutil.CPUSetAdjustmentMode,
) error {
	if target == nil {
		return nil
	}
	executionLease := cpuSetAdjustmentExecutionLeaseFromContext(ctx, p)
	if executionLease == nil {
		var err error
		executionLease, err = p.acquireCPUSetAdjustmentExecutionLocked(ctx)
		if err != nil {
			return fmt.Errorf("reconcile advisor post-commit target waiting for cpuset adjustment execution: %w", err)
		}
		defer executionLease.release()
		ctx = context.WithValue(ctx, cpuSetAdjustmentExecutionLeaseContextKey{}, executionLease)
	}
	p.cpuSetAdjustmentRetryMu.Lock()
	current := p.advisorPostCommitTarget == target
	abortPending := target.abortPending
	publicationPending := target.publicationPending
	applyMarkerPending := target.applyMarkerPending
	cleanupPending := target.cleanupPending
	applied := target.applied
	p.cpuSetAdjustmentRetryMu.Unlock()
	if !current {
		return nil
	}
	if abortPending {
		return p.retryAdvisorPostCommitAbort(target)
	}
	if publicationPending {
		if err := p.retryAdvisorPostCommitPublication(target); err != nil {
			return err
		}
	}
	if applyMarkerPending {
		if err := p.persistAdvisorPostCommitApplied(target); err != nil {
			return err
		}
		cleanupPending = true
	}
	if applied && !cleanupPending {
		p.cpuSetAdjustmentRetryMu.Lock()
		if p.advisorPostCommitTarget == target {
			target.cleanupPending = true
		}
		p.cpuSetAdjustmentRetryMu.Unlock()
		cleanupPending = true
	}
	if cleanupPending || applied {
		return p.completeAdvisorPostCommitCleanup(target)
	}
	if current, err := p.advisorPostCommitTargetStatus(target); err != nil {
		return err
	} else if !current {
		return nil
	}

	p.recordAdvisorPostCommitProgress(target, advisorPostCommitPhasePhysicalApply)
	mode := cpusetutil.CPUSetAdjustmentModePeriodic
	if len(modes) > 0 {
		mode = modes[0].OrFullDefault()
	}
	if err := p.applySteadyFakeNUMAMigrationCheckpointTransition(
		target.migrationCheckpointTransition); err != nil {
		if mode != cpusetutil.CPUSetAdjustmentModeRetry {
			p.markAdvisorApplyFailed(target.revision)
		}
		return fmt.Errorf("apply migration checkpoint transition failed: %w", err)
	}
	if current, err := p.advisorPostCommitTargetStatus(target); err != nil {
		return err
	} else if !current {
		return nil
	}
	headroomErr := p.applyHeadroom(target.response)
	if current, err := p.advisorPostCommitTargetStatus(target); err != nil {
		return err
	} else if !current {
		return nil
	}
	cgroupErr := p.applyCgroupConfigs(target.response)
	if current, err := p.advisorPostCommitTargetStatus(target); err != nil {
		return err
	} else if !current {
		return nil
	}
	adjustmentCtx := context.WithValue(ctx, advisorPostCommitTargetContextKey{}, target)
	adjustmentErr := p.runCPUSetAdjustmentHandlers(adjustmentCtx, mode)
	if headroomErr == nil && cgroupErr == nil && adjustmentErr == nil {
		p.cpuSetAdjustmentRetryMu.Lock()
		if p.advisorPostCommitTarget == target {
			target.applied = true
			target.applyMarkerPending = true
			p.recordAdvisorPostCommitProgressLocked(target, advisorPostCommitPhaseAppliedMarker)
		}
		p.cpuSetAdjustmentRetryMu.Unlock()
		if err := p.persistAdvisorPostCommitApplied(target); err != nil {
			return err
		}
		return p.completeAdvisorPostCommitCleanup(target)
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
	current, _ := p.advisorPostCommitTargetStatus(target)
	return current
}

func (p *DynamicPolicy) advisorPostCommitTargetStatus(
	target *advisorPostCommitTarget,
) (bool, error) {
	p.cpuSetAdjustmentRetryMu.Lock()
	current := p.advisorPostCommitTarget
	p.cpuSetAdjustmentRetryMu.Unlock()
	if current != target || target == nil || p.state == nil {
		return false, nil
	}
	actualRevision := p.state.GetRevision()
	if actualRevision != target.revision {
		return false, &advisorPostCommitPendingError{
			pendingRevision:   target.revision,
			attemptedRevision: actualRevision,
			source:            "advisor post-commit reconcile",
		}
	}
	return true, nil
}

func (p *DynamicPolicy) markAdvisorApplyFailed(revision uint64) {
	general.Errorf("post-advisor-commit apply failed for state revision %d; scheduling latest-state retry", revision)
	p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonApplyFailed)
}

func (p *DynamicPolicy) scheduleCPUSetAdjustmentPersistenceRetry() {
	p.cpuSetAdjustmentRetryMu.Lock()
	p.cpuSetAdjustmentRetryPersist = true
	p.cpuSetAdjustmentRetryMu.Unlock()
	p.scheduleCPUSetAdjustmentRetry(cpusetutil.RetryReasonPersistFailed)
}

func (p *DynamicPolicy) persistCPUSetAdjustmentStateIfNeeded() error {
	p.cpuSetAdjustmentRetryMu.Lock()
	pending := p.cpuSetAdjustmentRetryPersist
	p.cpuSetAdjustmentRetryMu.Unlock()
	if !pending {
		return nil
	}
	if err := p.state.StoreState(); err != nil {
		return fmt.Errorf("persist restored CPU state: %w", err)
	}
	p.cpuSetAdjustmentRetryMu.Lock()
	p.cpuSetAdjustmentRetryPersist = false
	delete(p.cpuSetAdjustmentRetryReasons, cpusetutil.RetryReasonPersistFailed)
	p.cpuSetAdjustmentRetryMu.Unlock()
	return nil
}

func (p *DynamicPolicy) retryLatestCPUSetAdjustment(
	ctx context.Context,
	mode cpusetutil.CPUSetAdjustmentMode,
) error {
	// The caller holds the policy lock, so persist a restored in-memory state
	// before any fallible cgroup reconciliation. Otherwise a persistent
	// adjustment failure can indefinitely leave the failed candidate on disk.
	persistErr := p.persistCPUSetAdjustmentStateIfNeeded()

	var adjustmentErr error
	if target := p.currentAdvisorPostCommitTarget(); target != nil {
		adjustmentErr = p.reconcileAdvisorPostCommitTarget(ctx, target, mode)
	} else {
		adjustmentErr = p.runCPUSetAdjustmentHandlers(ctx, mode)
	}
	if adjustmentErr != nil && persistErr != nil {
		return fmt.Errorf("%v; cpuset adjustment failed: %w", persistErr, adjustmentErr)
	}
	if adjustmentErr != nil {
		return adjustmentErr
	}
	return persistErr
}

func (p *DynamicPolicy) markCPUSetAdjustmentDirty(reason cpusetutil.CPUSetAdjustmentRetryReason) {
	p.cpuSetAdjustmentRetryMu.Lock()
	defer p.cpuSetAdjustmentRetryMu.Unlock()
	if p.cpuSetAdjustmentRetryStopping {
		return
	}
	p.cpuSetAdjustmentRetryDirty = true
	if p.cpuSetAdjustmentRetryReasons == nil {
		p.cpuSetAdjustmentRetryReasons = make(map[cpusetutil.CPUSetAdjustmentRetryReason]struct{})
	}
	p.cpuSetAdjustmentRetryReasons[reason] = struct{}{}
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
			err := p.retryLatestCPUSetAdjustment(ctx, cpusetutil.CPUSetAdjustmentModeRetry)
			cancel()
			p.Unlock()
			attempt++
			if err != nil {
				general.Errorf("retry latest cpuset adjustment failed, reason=%s: %v", reason, err)
			}

			p.cpuSetAdjustmentRetryMu.Lock()
			if p.cpuSetAdjustmentRetryStopping {
				p.cpuSetAdjustmentRetryQueued = false
				p.cpuSetAdjustmentRetryAgain = false
				p.cpuSetAdjustmentRetryMu.Unlock()
				return
			}
			retryAgain := p.cpuSetAdjustmentRetryAgain
			if retryAgain {
				p.cpuSetAdjustmentRetryAgain = false
			}
			if (err != nil || retryAgain) && attempt < cpuSetAdjustmentRetryMaxAttempts {
				p.cpuSetAdjustmentRetryMu.Unlock()
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
			if err == nil && !retryAgain && p.advisorPostCommitTarget == nil && !p.cpuSetAdjustmentRetryPersist {
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
	err := p.retryLatestCPUSetAdjustment(ctx, cpusetutil.CPUSetAdjustmentModePeriodic)
	cancel()
	p.Unlock()
	if err != nil {
		general.Errorf("periodic latest-state cpuset adjustment reconcile failed: %v", err)
	} else {
		p.cpuSetAdjustmentRetryMu.Lock()
		if p.advisorPostCommitTarget == nil && !p.cpuSetAdjustmentRetryPersist &&
			!p.cpuSetAdjustmentRetryQueued && !p.cpuSetAdjustmentRetryAgain {
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
