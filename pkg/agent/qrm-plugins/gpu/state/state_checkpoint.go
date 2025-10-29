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
	"errors"
	"fmt"
	"path"
	"reflect"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	cmerrors "k8s.io/kubernetes/pkg/kubelet/checkpointmanager/errors"

	"github.com/kubewharf/katalyst-core/pkg/config/agent/qrm"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

const (
	metricMetaCacheStoreStateDuration = "metacache_store_state_duration"
)

var (
	_          State = &stateCheckpoint{}
	generalLog       = general.LoggerWithPrefix("gpu_plugin", general.LoggingPKGFull)
)

// stateCheckpoint is an in-memory implementation of State;
// everytime we want to read or write states, those requests will always
// go to in-memory State, and then go to disk State, i.e. in write-back mode
type stateCheckpoint struct {
	sync.RWMutex
	cache             State
	policyName        string
	checkpointManager checkpointmanager.CheckpointManager
	checkpointName    string
	// when we add new properties to checkpoint,
	// it will cause checkpoint corruption and we should skip it
	skipStateCorruption bool
	emitter             metrics.MetricEmitter
}

func (s *stateCheckpoint) SetMachineState(allocationResourcesMap AllocationResourcesMap, persist bool) {
	s.Lock()
	defer s.Unlock()

	s.cache.SetMachineState(allocationResourcesMap, persist)
	if persist {
		err := s.storeState()
		if err != nil {
			generalLog.ErrorS(err, "store machineState to checkpoint error")
		}
	}
}

func (s *stateCheckpoint) SetResourceState(resourceName v1.ResourceName, allocationMap AllocationMap, persist bool) {
	s.Lock()
	defer s.Unlock()

	s.cache.SetResourceState(resourceName, allocationMap, persist)
	if persist {
		err := s.storeState()
		if err != nil {
			generalLog.ErrorS(err, "store resource state to checkpoint error")
		}
	}
}

func (s *stateCheckpoint) SetPodResourceEntries(podResourceEntries PodResourceEntries, persist bool) {
	s.Lock()
	defer s.Unlock()

	s.cache.SetPodResourceEntries(podResourceEntries, persist)
	if persist {
		err := s.storeState()
		if err != nil {
			generalLog.ErrorS(err, "store pod entries to checkpoint error", "err")
		}
	}
}

func (s *stateCheckpoint) SetAllocationInfo(
	resourceName v1.ResourceName, podUID, containerName string, allocationInfo *AllocationInfo, persist bool,
) {
	s.Lock()
	defer s.Unlock()

	s.cache.SetAllocationInfo(resourceName, podUID, containerName, allocationInfo, persist)
	if persist {
		err := s.storeState()
		if err != nil {
			generalLog.ErrorS(err, "store allocationInfo to checkpoint error")
		}
	}
}

func (s *stateCheckpoint) Delete(resourceName v1.ResourceName, podUID, containerName string, persist bool) {
	s.Lock()
	defer s.Unlock()

	s.cache.Delete(resourceName, podUID, containerName, persist)
	if persist {
		err := s.storeState()
		if err != nil {
			generalLog.ErrorS(err, "store state after delete operation to checkpoint error")
		}
	}
}

func (s *stateCheckpoint) ClearState() {
	s.Lock()
	defer s.Unlock()

	s.cache.ClearState()
	err := s.storeState()
	if err != nil {
		generalLog.ErrorS(err, "store state after clear operation to checkpoint error")
	}
}

func (s *stateCheckpoint) StoreState() error {
	s.Lock()
	defer s.Unlock()
	return s.storeState()
}

func (s *stateCheckpoint) GetMachineState() AllocationResourcesMap {
	s.RLock()
	defer s.RUnlock()

	return s.cache.GetMachineState()
}

func (s *stateCheckpoint) GetPodResourceEntries() PodResourceEntries {
	s.RLock()
	defer s.RUnlock()

	return s.cache.GetPodResourceEntries()
}

func (s *stateCheckpoint) GetPodEntries(resourceName v1.ResourceName) PodEntries {
	s.RLock()
	defer s.RUnlock()

	return s.cache.GetPodEntries(resourceName)
}

func (s *stateCheckpoint) GetAllocationInfo(
	resourceName v1.ResourceName, podUID, containerName string,
) *AllocationInfo {
	s.RLock()
	defer s.RUnlock()

	return s.cache.GetAllocationInfo(resourceName, podUID, containerName)
}

func (s *stateCheckpoint) storeState() error {
	startTime := time.Now()
	general.InfoS("called")
	defer func() {
		elapsed := time.Since(startTime)
		general.InfoS("finished", "duration", elapsed)
		_ = s.emitter.StoreFloat64(metricMetaCacheStoreStateDuration, float64(elapsed/time.Millisecond), metrics.MetricTypeNameRaw)
	}()
	checkpoint := NewGPUPluginCheckpoint()
	checkpoint.PolicyName = s.policyName
	checkpoint.MachineState = s.cache.GetMachineState()
	checkpoint.PodResourceEntries = s.cache.GetPodResourceEntries()

	err := s.checkpointManager.CreateCheckpoint(s.checkpointName, checkpoint)
	if err != nil {
		generalLog.ErrorS(err, "could not save checkpoint")
		return err
	}
	return nil
}

func (s *stateCheckpoint) restoreState(defaultResourceStateGenerators *DefaultResourceStateGeneratorRegistry) error {
	s.Lock()
	defer s.Unlock()
	var err error
	var foundAndSkippedStateCorruption bool

	checkpoint := NewGPUPluginCheckpoint()
	if err = s.checkpointManager.GetCheckpoint(s.checkpointName, checkpoint); err != nil {
		if errors.Is(err, cmerrors.ErrCheckpointNotFound) {
			return s.storeState()
		} else if errors.Is(err, cmerrors.ErrCorruptCheckpoint) {
			if !s.skipStateCorruption {
				return err
			}

			foundAndSkippedStateCorruption = true
			generalLog.Infof("restore checkpoint failed with err: %s, but we skip it", err)
		} else {
			return err
		}
	}

	if s.policyName != checkpoint.PolicyName && !s.skipStateCorruption {
		return fmt.Errorf("configured policy %q differs from state checkpoint policy %q", s.policyName, checkpoint.PolicyName)
	}

	machineState, err := GenerateMachineStateFromPodEntries(checkpoint.PodResourceEntries, defaultResourceStateGenerators)
	if err != nil {
		return fmt.Errorf("GenerateMachineStateFromPodEntries failed with error: %v", err)
	}

	s.cache.SetMachineState(machineState, false)
	s.cache.SetPodResourceEntries(checkpoint.PodResourceEntries, false)

	if !reflect.DeepEqual(machineState, checkpoint.MachineState) {
		generalLog.Warningf("machine state changed: "+
			"machineState: %s; checkpointMachineState: %s",
			machineState.String(), checkpoint.MachineState.String())

		err = s.storeState()
		if err != nil {
			return fmt.Errorf("storeState when machine state changed failed with error: %v", err)
		}
	}

	if foundAndSkippedStateCorruption {
		generalLog.Infof("found and skipped state corruption, we shoud store to rectify the checksum")

		err = s.storeState()
		if err != nil {
			return fmt.Errorf("storeState failed with error: %v", err)
		}
	}

	generalLog.InfoS("state checkpoint: restored state from checkpoint")

	return nil
}

func NewCheckpointState(
	conf *qrm.QRMPluginsConfiguration, stateDir, checkpointName, policyName string,
	defaultResourceStateGenerators *DefaultResourceStateGeneratorRegistry,
	skipStateCorruption bool, emitter metrics.MetricEmitter,
) (State, error) {
	checkpointManager, err := checkpointmanager.NewCheckpointManager(stateDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize checkpoint manager: %v", err)
	}

	defaultCache, err := NewGPUPluginState(conf, defaultResourceStateGenerators)
	if err != nil {
		return nil, fmt.Errorf("NewGPUPluginState failed with error: %v", err)
	}

	sc := &stateCheckpoint{
		cache:               defaultCache,
		policyName:          policyName,
		checkpointManager:   checkpointManager,
		checkpointName:      checkpointName,
		skipStateCorruption: skipStateCorruption,
		emitter:             emitter,
	}

	if err := sc.restoreState(defaultResourceStateGenerators); err != nil {
		return nil, fmt.Errorf("could not restore state from checkpoint: %v, please drain this node and delete "+
			"the gpu plugin checkpoint file %q before restarting Kubelet",
			err, path.Join(stateDir, checkpointName))
	}

	return sc, nil
}
