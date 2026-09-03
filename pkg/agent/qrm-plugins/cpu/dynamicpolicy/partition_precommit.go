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
	"fmt"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type pendingCPUPartition struct {
	expectedRevision     uint64
	entries              state.PodEntries
	baseMachineState     state.NUMANodeMap
	allowOverlap         bool
	disableDedicated     bool
	persist              bool
	source               string
	validate             func(state.PodEntries, state.NUMANodeMap, bool, bool) error
	enforceSteadyReclaim bool
}

type preparedCPUPartition struct {
	pending      pendingCPUPartition
	entries      state.PodEntries
	machineState state.NUMANodeMap
}

// preparePendingCPUPartition performs every fallible precommit step without
// mutating plugin state: clone, hooks, normalize, rebuild, and validation.
func (p *DynamicPolicy) preparePendingCPUPartition(
	pending pendingCPUPartition,
) (*preparedCPUPartition, error) {
	if p == nil || p.state == nil {
		return nil, fmt.Errorf("prepare pending cpu partition: policy is not initialized")
	}
	if pending.entries == nil {
		return nil, fmt.Errorf("prepare pending cpu partition: entries are nil")
	}

	currentEntries := p.state.GetPodEntries()
	candidate := pending.entries.Clone()
	if err := p.invokeAllocationHooksForPodEntries(currentEntries, candidate); err != nil {
		return nil, p.wrapPartitionPrecommitError(pending.source, "run allocation hooks", err)
	}
	var machineState state.NUMANodeMap
	if p.machineInfo == nil || p.machineInfo.CPUTopology == nil {
		if pending.baseMachineState == nil {
			return nil, fmt.Errorf("prepare pending cpu partition: cpu topology is not initialized")
		}
		machineState = pending.baseMachineState.Clone()
	} else {
		if err := p.normalizePendingCPUPartition(candidate); err != nil {
			return nil, p.wrapPartitionPrecommitError(pending.source, "normalize allocation entries", err)
		}
		if err := validateAllocationShapeAfterHooks(
			pending.entries, candidate, p.machineInfo.CPUTopology,
		); err != nil {
			return nil, p.wrapPartitionPrecommitError(
				pending.source, "revalidate allocation shape after hooks", err)
		}
		if pending.enforceSteadyReclaim {
			plannedReclaim := reclaimPoolCPUSet(pending.entries)
			candidateReclaim := reclaimPoolCPUSet(candidate)
			committedReclaim := reclaimPoolCPUSet(currentEntries)
			if err := validateSteadyReclaimPrecommitInvariant(
				plannedReclaim, candidateReclaim, committedReclaim,
				p.machineInfo.CPUTopology,
			); err != nil {
				return nil, p.wrapPartitionPrecommitError(
					pending.source, "revalidate steady reclaim after hooks", err)
			}
		}
		baseMachineState := pending.baseMachineState
		if baseMachineState == nil {
			baseMachineState = p.state.GetMachineState()
		} else {
			baseMachineState = baseMachineState.Clone()
		}
		var err error
		machineState, err = generateMachineStateFromPodEntries(
			p.machineInfo.CPUTopology, candidate, baseMachineState)
		if err != nil {
			return nil, p.wrapPartitionPrecommitError(pending.source, "rebuild machine state", err)
		}
	}
	validate := pending.validate
	if validate == nil {
		validate = p.validateAdvisorPartitionBeforeCommit
	}
	if err := validate(candidate, machineState, pending.allowOverlap, pending.disableDedicated); err != nil {
		return nil, p.wrapPartitionPrecommitError(pending.source, "validate hard floor and partition", err)
	}
	return &preparedCPUPartition{
		pending:      pending,
		entries:      candidate,
		machineState: machineState,
	}, nil
}

// commitPreparedCPUPartition is the only state mutation for a prepared CPU
// partition. No fallible response or finalize work may follow this revision-CAS.
func (p *DynamicPolicy) commitPreparedCPUPartition(prepared *preparedCPUPartition) error {
	if prepared == nil {
		return fmt.Errorf("commit prepared cpu partition: candidate is nil")
	}
	if err := p.state.CommitAdvisorStateIfRevision(
		prepared.pending.expectedRevision,
		prepared.entries,
		prepared.machineState,
		prepared.pending.allowOverlap,
		prepared.pending.disableDedicated,
		prepared.pending.persist,
	); err != nil {
		return err
	}
	p.emitFinalPoolSizeMetrics(prepared.entries)
	return nil
}

func (p *DynamicPolicy) commitPendingCPUPartition(
	pending pendingCPUPartition,
) (state.PodEntries, state.NUMANodeMap, error) {
	prepared, err := p.preparePendingCPUPartition(pending)
	if err != nil {
		return nil, nil, err
	}
	if err := p.commitPreparedCPUPartition(prepared); err != nil {
		return nil, nil, err
	}
	return prepared.entries, prepared.machineState, nil
}

func (p *DynamicPolicy) normalizePendingCPUPartition(entries state.PodEntries) error {
	for podUID, containerEntries := range entries {
		for containerName, allocation := range containerEntries {
			if allocation == nil {
				continue
			}
			assignments, err := machine.GetNumaAwareAssignments(
				p.machineInfo.CPUTopology, allocation.AllocationResult)
			if err != nil {
				return fmt.Errorf("%s/%s allocation result: %w", podUID, containerName, err)
			}
			originalAssignments, err := machine.GetNumaAwareAssignments(
				p.machineInfo.CPUTopology, allocation.OriginalAllocationResult)
			if err != nil {
				return fmt.Errorf("%s/%s original allocation result: %w", podUID, containerName, err)
			}
			allocation.TopologyAwareAssignments = assignments
			allocation.OriginalTopologyAwareAssignments = originalAssignments
		}
	}
	return nil
}

func validateAllocationShapeAfterHooks(
	planned, candidate state.PodEntries,
	topology *machine.CPUTopology,
) error {
	for podUID, plannedContainers := range planned {
		for containerName, plannedAllocation := range plannedContainers {
			if plannedAllocation == nil {
				continue
			}
			candidateAllocation := candidate[podUID][containerName]
			if candidateAllocation == nil {
				return fmt.Errorf("allocation %s/%s was removed after hooks", podUID, containerName)
			}
			if plannedAllocation.AllocationResult.Size() != candidateAllocation.AllocationResult.Size() {
				return fmt.Errorf(
					"allocation quantity changed after hooks for %s/%s: planned=%d candidate=%d",
					podUID, containerName,
					plannedAllocation.AllocationResult.Size(),
					candidateAllocation.AllocationResult.Size())
			}
			for _, numaID := range topology.CPUDetails.NUMANodes().ToSliceInt() {
				numaCPUs := topology.CPUDetails.CPUsInNUMANodes(numaID)
				plannedQuantity := plannedAllocation.AllocationResult.Intersection(numaCPUs).Size()
				candidateQuantity := candidateAllocation.AllocationResult.Intersection(numaCPUs).Size()
				if plannedQuantity != candidateQuantity {
					return fmt.Errorf(
						"allocation NUMA distribution changed after hooks for %s/%s NUMA %d: planned=%d candidate=%d",
						podUID, containerName, numaID, plannedQuantity, candidateQuantity)
				}
			}
		}
	}
	return nil
}

func reclaimPoolCPUSet(entries state.PodEntries) machine.CPUSet {
	if reclaimEntries := entries[commonstate.PoolNameReclaim]; reclaimEntries != nil {
		if reclaim := reclaimEntries[commonstate.FakedContainerName]; reclaim != nil {
			return reclaim.AllocationResult.Clone()
		}
	}
	return machine.NewCPUSet()
}

func validateSteadyReclaimPrecommitInvariant(
	planned, candidate, committed machine.CPUSet,
	topology *machine.CPUTopology,
) error {
	if topology == nil {
		return fmt.Errorf("steady reclaim precommit topology is nil")
	}
	if err := assertCoreAligned(candidate, topology); err != nil {
		return fmt.Errorf("steady reclaim is not core-aligned: %w", err)
	}
	if candidate.Size() != planned.Size() {
		return fmt.Errorf(
			"steady reclaim quantity changed after hooks: planned=%d candidate=%d",
			planned.Size(), candidate.Size())
	}
	if churn := steadyFakeNUMAMigrationChurn(committed, candidate); churn >
		steadyFakeNUMAMaxMigratedCPUs {
		return fmt.Errorf(
			"steady reclaim migration churn %d exceeds limit %d",
			churn, steadyFakeNUMAMaxMigratedCPUs)
	}
	for _, numaID := range topology.CPUDetails.NUMANodes().ToSliceInt() {
		numaCPUs := topology.CPUDetails.CPUsInNUMANodes(numaID)
		plannedQuantity := planned.Intersection(numaCPUs).Size()
		candidateQuantity := candidate.Intersection(numaCPUs).Size()
		if plannedQuantity != candidateQuantity {
			return fmt.Errorf(
				"steady reclaim NUMA distribution changed after hooks for NUMA %d: planned=%d candidate=%d",
				numaID, plannedQuantity, candidateQuantity)
		}
	}
	return nil
}

func (p *DynamicPolicy) wrapPartitionPrecommitError(source, stage string, err error) error {
	if source == "" {
		source = "cpu partition"
	}
	return fmt.Errorf("%s precommit %s: %w", source, stage, err)
}
