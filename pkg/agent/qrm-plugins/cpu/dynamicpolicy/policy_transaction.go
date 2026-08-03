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

package dynamicpolicy

import (
	"context"
	"fmt"

	apiequality "k8s.io/apimachinery/pkg/api/equality"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/planner"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type TargetPlan func(base *state.TargetState) (*state.TargetState, error)

// StaleTargetBaseError reports that work planned from a captured durable base
// can no longer be committed because another transaction advanced the base.
type StaleTargetBaseError struct{}

func (*StaleTargetBaseError) Error() string {
	return "durable target base changed while work was in flight"
}

// targetMutationEditor is deliberately narrower than DynamicPolicy. Mutation
// helpers receive it explicitly and may only change the owned candidate.
type targetMutationEditor struct {
	target *state.TargetState
}

func newTargetMutationEditor(target *state.TargetState) *targetMutationEditor {
	return &targetMutationEditor{target: target}
}

func putTargetAllocation(target *state.TargetState, podUID, containerName string, allocationInfo *state.AllocationInfo) {
	if allocationInfo == nil {
		return
	}
	if target.PodEntries == nil {
		target.PodEntries = make(state.PodEntries)
	}
	if target.PodEntries[podUID] == nil {
		target.PodEntries[podUID] = make(state.ContainerEntries)
	}
	target.PodEntries[podUID][containerName] = allocationInfo.Clone()
}

func deleteTargetAllocation(target *state.TargetState, podUID, containerName string) {
	if target.PodEntries == nil || target.PodEntries[podUID] == nil {
		return
	}
	delete(target.PodEntries[podUID], containerName)
	if len(target.PodEntries[podUID]) == 0 {
		delete(target.PodEntries, podUID)
	}
}

func (p *DynamicPolicy) transact(
	ctx context.Context,
	plan TargetPlan,
	postCommit ...func(committedRevision uint64),
) error {
	return p.transactOwned(ctx, true, nil, nil, plan, firstCommittedPostCommit(postCommit))
}

func (p *DynamicPolicy) transactWithPostCommit(
	ctx context.Context,
	plan TargetPlan,
	postCommit func(),
) error {
	if postCommit == nil {
		return p.transactOwned(ctx, true, nil, nil, plan, nil)
	}
	return p.transactOwned(ctx, true, nil, nil, plan, func(uint64) {
		postCommit()
	})
}

func (p *DynamicPolicy) transactBootstrap(ctx context.Context, plan TargetPlan) error {
	return p.transactOwned(ctx, false, nil, nil, plan, nil)
}

func (p *DynamicPolicy) transactIfBaseUnchanged(
	ctx context.Context,
	capturedBase *state.TargetState,
	plan TargetPlan,
) error {
	if capturedBase == nil {
		return fmt.Errorf("captured durable target base is nil")
	}
	return p.transactOwned(ctx, true, capturedBase, nil, plan, nil)
}

func (p *DynamicPolicy) transactIfAdviceFresh(
	ctx context.Context,
	pending planner.PendingAdviceSnapshot,
	plan TargetPlan,
	postCommit ...func(committedRevision uint64),
) error {
	return p.transactOwned(ctx, true, nil, &pending, plan, firstCommittedPostCommit(postCommit))
}

func firstCommittedPostCommit(callbacks []func(uint64)) func(uint64) {
	if len(callbacks) == 0 {
		return nil
	}
	return callbacks[0]
}

func (p *DynamicPolicy) transactOwned(
	ctx context.Context,
	requireReady bool,
	capturedBase *state.TargetState,
	pendingAdvice *planner.PendingAdviceSnapshot,
	plan TargetPlan,
	postCommit func(committedRevision uint64),
) error {
	p.Lock()
	defer p.Unlock()

	if requireReady {
		if err := p.requireReadyLocked(); err != nil {
			return err
		}
	}
	if plan == nil {
		return fmt.Errorf("target plan is nil")
	}

	base, err := p.state.PrepareDurableTarget()
	if err != nil {
		return fmt.Errorf("prepare durable target: %w", err)
	}
	if base == nil {
		return fmt.Errorf("prepare durable target returned nil")
	}
	if pendingAdvice != nil {
		request, requestErr := p.createGetAdviceRequestForTarget(base)
		if requestErr != nil {
			return fmt.Errorf("rebuild GetAdviceRequest: %w", requestErr)
		}
		requestHash, requestErr := normalizedGetAdviceRequestHash(request)
		if requestErr != nil {
			return fmt.Errorf("hash rebuilt GetAdviceRequest: %w", requestErr)
		}
		if freshnessErr := pendingAdvice.Validate(planner.AdviceFreshness{
			Token:                 p.advisorToken,
			InMemoryRevision:      p.inMemoryRevision,
			NormalizedRequestHash: requestHash,
		}); freshnessErr != nil {
			return freshnessErr
		}
	}
	if capturedBase != nil && !apiequality.Semantic.DeepEqual(base, capturedBase) {
		return &StaleTargetBaseError{}
	}
	hardPartitionEnabled := p.isRampUpReclaimHardPartitionEnabled()
	requiredFloor := planner.ActiveRampUpReclaimFloor(base, p.cpuTopology(), hardPartitionEnabled)

	// The planner receives its own owned copy. Keep base pristine so recovery
	// always converges the exact durable snapshot, even when planning is dirty.
	next, err := plan(base.Clone())
	if err != nil {
		return err
	}
	next.MachineState, err = state.GenerateMachineStateFromPodEntries(
		p.cpuTopology(), next.PodEntries, next.MachineState)
	if err != nil {
		return fmt.Errorf("recalculate target machine state: %w", err)
	}
	requireDisjoint := p.requiresReclaimDisjoint(next.AllowSharedCoresOverlapReclaimedCores)
	if err := planner.ValidateTarget(next, p.cpuTopology(), requiredFloor, requireDisjoint); err != nil {
		return fmt.Errorf("validate target: %w", err)
	}

	materializationTarget, err := BuildMaterializationTarget(
		next, p.cpuTopology(), p.effectiveReclaimOverlap(next.AllowSharedCoresOverlapReclaimedCores))
	if err != nil {
		return fmt.Errorf("build materialization target: %w", err)
	}
	if err := p.materialize(ctx, materializationTarget); err != nil {
		return p.restoreBaseOrBlock(ctx, base, fmt.Errorf("materialize target: %w", err))
	}
	outbox := p.advisorCgroupPostCommitOutboxInstance()
	if err := outbox.linearizeTargetCommit(ctx, func() (uint64, error) {
		if commitErr := p.state.CommitTarget(next); commitErr != nil {
			return 0, commitErr
		}
		p.inMemoryRevision++
		return p.inMemoryRevision, nil
	}); err != nil {
		return p.restoreBaseOrBlock(ctx, base, fmt.Errorf("commit target: %w", err))
	}
	if postCommit != nil {
		postCommit(p.inMemoryRevision)
	}
	return nil
}

func (p *DynamicPolicy) materialize(ctx context.Context, target cpusetmaterializer.Target) error {
	if p.cpuSetMaterializer == nil {
		return nil
	}
	result, err := p.cpuSetMaterializer.Materialize(ctx, target)
	if err != nil {
		return err
	}
	if !result.Converged {
		return cpusetmaterializer.ErrCPUSetNotConverged
	}
	return nil
}

func (p *DynamicPolicy) cpuTopology() *machine.CPUTopology {
	if p.machineInfo == nil {
		return nil
	}
	return p.machineInfo.CPUTopology
}
