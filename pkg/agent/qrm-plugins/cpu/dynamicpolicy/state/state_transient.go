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

package state

import "github.com/kubewharf/katalyst-core/pkg/util/machine"

// transientState adapts the in-memory store to State for speculative planning.
// Persistence flags are intentionally ignored and StoreState is a no-op.
type transientState struct {
	*cpuPluginState
}

func NewTransientState(topology *machine.CPUTopology) State {
	return &transientState{cpuPluginState: NewCPUPluginState(topology)}
}

func (s *transientState) SetMachineState(v NUMANodeMap, _ bool) {
	s.cpuPluginState.SetMachineState(v)
}

func (s *transientState) SetNUMAHeadroom(v map[int]float64, _ bool) {
	s.cpuPluginState.SetNUMAHeadroom(v)
}

func (s *transientState) SetPodEntries(v PodEntries, _ bool) {
	s.cpuPluginState.SetPodEntries(v)
}

func (s *transientState) SetAllocationInfo(podUID, containerName string, allocation *AllocationInfo, _ bool) {
	s.cpuPluginState.SetAllocationInfo(podUID, containerName, allocation)
}

func (s *transientState) SetAllowSharedCoresOverlapReclaimedCores(v bool, _ bool) {
	s.cpuPluginState.SetAllowSharedCoresOverlapReclaimedCores(v)
}

func (s *transientState) SetDisableDedicatedCoresOverlapReclaimedCores(v bool, _ bool) {
	s.cpuPluginState.SetDisableDedicatedCoresOverlapReclaimedCores(v)
}

func (s *transientState) CommitAdvisorState(
	podEntries PodEntries,
	machineState NUMANodeMap,
	allowOverlap, disableDedicatedOverlap bool,
	_ bool,
	defaultShareMaterializationState DefaultShareMaterializationState,
) error {
	return s.cpuPluginState.CommitAdvisorState(
		podEntries, machineState, allowOverlap, disableDedicatedOverlap, false,
		defaultShareMaterializationState)
}

func (s *transientState) CommitAdvisorStateIfRevision(
	expectedRevision uint64,
	podEntries PodEntries,
	machineState NUMANodeMap,
	allowOverlap, disableDedicatedOverlap bool,
	_ bool,
	defaultShareMaterializationState DefaultShareMaterializationState,
) error {
	return s.cpuPluginState.CommitAdvisorStateIfRevision(
		expectedRevision, podEntries, machineState, allowOverlap, disableDedicatedOverlap, false,
		defaultShareMaterializationState)
}

func (s *transientState) Delete(podUID, containerName string, _ bool) {
	s.cpuPluginState.Delete(podUID, containerName)
}

func (s *transientState) StoreState() error {
	return nil
}
