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

package state

import (
	"fmt"
	"path"
	"reflect"
	"sync"
	"time"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"

	"github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/statedirectory"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/customcheckpointmanager"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	"github.com/kubewharf/katalyst-core/pkg/util/state"
)

const (
	metricMetaCacheStoreStateDuration = "metacache_store_state_duration"
)

// stateCheckpoint is an in-memory implementation of State;
// everytime we want to read or write states, those requests will always
// go to in-memory State, and then go to disk State, i.e. in write-back mode
type stateCheckpoint struct {
	sync.RWMutex
	cache             *cpuPluginState
	cacheDurable      bool
	policyName        string
	checkpointManager checkpointmanager.CheckpointManager
	checkpointName    string
	// when we add new properties to checkpoint,
	// it will cause checkpoint corruption, and we should skip it
	skipStateCorruption                bool
	GenerateMachineStateFromPodEntries GenerateMachineStateFromPodEntriesFunc
	emitter                            metrics.MetricEmitter
	topology                           *machine.CPUTopology
}

var (
	_ State          = &stateCheckpoint{}
	_ state.Storable = &stateCheckpoint{}
)

func NewCheckpointState(
	stateDirectoryConfig *statedirectory.StateDirectoryConfiguration, checkpointName, policyName string,
	topology *machine.CPUTopology, skipStateCorruption bool,
	generateMachineStateFunc GenerateMachineStateFromPodEntriesFunc,
	emitter metrics.MetricEmitter,
) (State, error) {
	currentStateDir, otherStateDir := stateDirectoryConfig.GetCurrentAndPreviousStateFileDirectory()

	sc := &stateCheckpoint{
		cache:                              NewCPUPluginState(topology),
		policyName:                         policyName,
		checkpointName:                     checkpointName,
		skipStateCorruption:                skipStateCorruption,
		GenerateMachineStateFromPodEntries: generateMachineStateFunc,
		emitter:                            emitter,
		topology:                           topology,
	}

	cm, err := customcheckpointmanager.NewCustomCheckpointManager(currentStateDir, otherStateDir, checkpointName,
		"cpu_plugin", sc, skipStateCorruption)
	if err != nil {
		return nil, fmt.Errorf("could not restore state from checkpoint: %v, please drain this node and delete "+
			"the cpu plugin checkpoint file %q before restarting Kubelet", err, path.Join(currentStateDir, checkpointName))
	}

	sc.checkpointManager = cm
	sc.cacheDurable = true

	return sc, nil
}

// RestoreState implements Storable interface and restores the cache from checkpoint and returns if the state has changed.
func (sc *stateCheckpoint) RestoreState(cp checkpointmanager.Checkpoint) (bool, error) {
	checkpoint, ok := cp.(*CPUPluginCheckpoint)
	if !ok {
		return false, fmt.Errorf("checkpoint type assertion failed, expect *CPUPluginCheckpoint, got %T", cp)
	}

	if sc.policyName != checkpoint.PolicyName && !sc.skipStateCorruption {
		return false, fmt.Errorf("[cpu_plugin] configured policy %q differs from state checkpoint policy %q", sc.policyName, checkpoint.PolicyName)
	}

	generatedMachineState, err := sc.GenerateMachineStateFromPodEntries(sc.topology, checkpoint.PodEntries, checkpoint.MachineState)
	if err != nil {
		return false, fmt.Errorf("GenerateMachineStateFromPodEntries failed with error: %v", err)
	}

	sc.cache.replaceOwnedTarget((&TargetState{
		MachineState:                               generatedMachineState,
		PodEntries:                                 checkpoint.PodEntries,
		NUMAHeadroom:                               checkpoint.NUMAHeadroom,
		AllowSharedCoresOverlapReclaimedCores:      checkpoint.AllowSharedCoresOverlapReclaimedCores,
		DisableDedicatedCoresOverlapReclaimedCores: checkpoint.DisableDedicatedCoresOverlapReclaimedCores,
	}).Clone())

	if !reflect.DeepEqual(generatedMachineState, checkpoint.MachineState) {
		klog.Warningf("[cpu_plugin] machine state changed: generatedMachineState: %s; checkpointMachineState: %s",
			generatedMachineState.String(), checkpoint.MachineState.String())

		return true, nil
	}

	return false, nil
}

func (sc *stateCheckpoint) checkpointFromTarget(target *TargetState) *CPUPluginCheckpoint {
	checkpoint := NewCPUPluginCheckpoint()
	checkpoint.PolicyName = sc.policyName
	if target == nil {
		return checkpoint
	}
	checkpoint.MachineState = target.MachineState
	checkpoint.NUMAHeadroom = target.NUMAHeadroom
	checkpoint.PodEntries = target.PodEntries
	checkpoint.AllowSharedCoresOverlapReclaimedCores = target.AllowSharedCoresOverlapReclaimedCores
	checkpoint.DisableDedicatedCoresOverlapReclaimedCores = target.DisableDedicatedCoresOverlapReclaimedCores
	return checkpoint
}

func (sc *stateCheckpoint) writeTargetCheckpoint(target *TargetState) error {
	startTime := time.Now()
	general.InfoS("called")
	defer func() {
		elapsed := time.Since(startTime)
		general.InfoS("finished", "duration", elapsed)
		_ = sc.emitter.StoreFloat64(metricMetaCacheStoreStateDuration, float64(elapsed/time.Millisecond), metrics.MetricTypeNameRaw)
	}()

	err := sc.checkpointManager.CreateCheckpoint(sc.checkpointName, sc.checkpointFromTarget(target))
	if err != nil {
		klog.ErrorS(err, "Could not save checkpoint")
		return err
	}
	return nil
}

// InitNewCheckpoint implements Storable interface and initializes an empty or non-empty new checkpoint.
func (sc *stateCheckpoint) InitNewCheckpoint(empty bool) checkpointmanager.Checkpoint {
	checkpoint := NewCPUPluginCheckpoint()
	if empty {
		return checkpoint
	}
	return sc.checkpointFromTarget(sc.cache.snapshot())
}

// PrepareDurableTarget returns an owned cache snapshot that exactly matches
// the checkpoint. A clean cache avoids an unnecessary checkpoint write.
func (sc *stateCheckpoint) PrepareDurableTarget() (*TargetState, error) {
	sc.Lock()
	defer sc.Unlock()

	base := sc.cache.snapshot()
	if sc.cacheDurable {
		return base, nil
	}
	if err := sc.writeTargetCheckpoint(base); err != nil {
		return nil, err
	}
	sc.cacheDurable = true
	return base, nil
}

// CommitTarget persists a defensive clone before atomically publishing it.
func (sc *stateCheckpoint) CommitTarget(next *TargetState) error {
	sc.Lock()
	defer sc.Unlock()

	if next == nil {
		return fmt.Errorf("cannot commit nil target")
	}
	owned := next.Clone()
	if err := sc.writeTargetCheckpoint(owned); err != nil {
		return err
	}
	sc.cache.replaceOwnedTarget(owned)
	sc.cacheDurable = true
	return nil
}

func (sc *stateCheckpoint) GetMachineState() NUMANodeMap {
	sc.RLock()
	defer sc.RUnlock()

	return sc.cache.GetMachineState()
}

func (sc *stateCheckpoint) GetNUMAHeadroom() map[int]float64 {
	sc.RLock()
	defer sc.RUnlock()

	return sc.cache.GetNUMAHeadroom()
}

func (sc *stateCheckpoint) GetAllocationInfo(podUID string, containerName string) *AllocationInfo {
	sc.RLock()
	defer sc.RUnlock()

	return sc.cache.GetAllocationInfo(podUID, containerName)
}

func (sc *stateCheckpoint) GetPodEntries() PodEntries {
	sc.RLock()
	defer sc.RUnlock()

	return sc.cache.GetPodEntries()
}

func (sc *stateCheckpoint) GetAllowSharedCoresOverlapReclaimedCores() bool {
	sc.RLock()
	defer sc.RUnlock()

	return sc.cache.GetAllowSharedCoresOverlapReclaimedCores()
}

func (sc *stateCheckpoint) GetDisableDedicatedCoresOverlapReclaimedCores() bool {
	sc.RLock()
	defer sc.RUnlock()

	return sc.cache.GetDisableDedicatedCoresOverlapReclaimedCores()
}
