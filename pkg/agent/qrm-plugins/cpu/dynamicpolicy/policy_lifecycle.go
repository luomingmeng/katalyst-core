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
	"errors"
	"fmt"
	"time"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/config"
	dynamicconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
)

const restoreBaseTimeout = 5 * time.Second

// joinedRestoreError is the Go 1.18-compatible equivalent of errors.Join for
// the two errors that define a failed rollback transaction.
type joinedRestoreError struct {
	cause   error
	restore error
}

func (e *joinedRestoreError) Error() string {
	return e.cause.Error() + "\n" + e.restore.Error()
}

func (e *joinedRestoreError) Unwrap() error {
	return e.cause
}

func (e *joinedRestoreError) Is(target error) bool {
	return errors.Is(e.cause, target) || errors.Is(e.restore, target)
}

// restoreBaseOrBlock strongly converges the durable base after a candidate may
// have partially changed external state. Mutation callers own the policy write
// lock, so lifecycleState and lifecycleErr are changed atomically with respect
// to every gated mutation.
func (p *DynamicPolicy) restoreBaseOrBlock(_ context.Context, base *state.TargetState, cause error) error {
	if cause == nil {
		return nil
	}

	restoreCtx, cancel := context.WithTimeout(context.Background(), restoreBaseTimeout)
	defer cancel()

	baseTarget, restoreErr := BuildMaterializationTarget(
		base, p.cpuTopology(), p.effectiveReclaimOverlapForTarget(base))
	if restoreErr == nil {
		restoreErr = p.materialize(restoreCtx, baseTarget)
	}
	if restoreErr == nil {
		return cause
	}

	joined := &joinedRestoreError{
		cause:   cause,
		restore: fmt.Errorf("restore durable base through bulkhead: %w", restoreErr),
	}
	p.lifecycleState = policyLifecycleBlocked
	p.lifecycleErr = joined
	return joined
}

type policyLifecycleState uint8

const (
	policyLifecycleUnknown policyLifecycleState = iota
	policyLifecycleRecovering
	policyLifecycleReady
	policyLifecycleBlocked
)

type policyComponentStopper struct {
	name string
	stop func() error
}

type policyStartupComponent struct {
	name  string
	start func() (policyComponentStopper, error)
}

func stopPolicyChannel(stopCh chan struct{}) error {
	select {
	case <-stopCh:
	default:
		close(stopCh)
	}
	return nil
}

func (p *DynamicPolicy) recordStartedComponent(stopper policyComponentStopper) {
	if stopper.stop == nil {
		return
	}
	p.Lock()
	p.startedComponentStoppers = append(p.startedComponentStoppers, stopper)
	p.Unlock()
}

func (p *DynamicPolicy) startComponents(components []policyStartupComponent) error {
	for _, component := range components {
		stopper, err := component.start()
		if err != nil {
			stopErr := p.stopStartedComponents()
			if stopErr != nil {
				return fmt.Errorf("start component %q: %w; rollback: %v", component.name, err, stopErr)
			}
			return fmt.Errorf("start component %q: %w", component.name, err)
		}
		if stopper.stop == nil {
			continue
		}
		if stopper.name == "" {
			stopper.name = component.name
		}
		p.recordStartedComponent(stopper)
	}
	return nil
}

func (p *DynamicPolicy) stopStartedComponents() error {
	p.Lock()
	stoppers := p.startedComponentStoppers
	p.startedComponentStoppers = nil
	p.Unlock()

	var stopErrs []error
	for i := len(stoppers) - 1; i >= 0; i-- {
		if err := stoppers[i].stop(); err != nil {
			stopErrs = append(stopErrs, fmt.Errorf("stop component %q: %w", stoppers[i].name, err))
		}
	}
	return utilerrors.NewAggregate(stopErrs)
}

func (s policyLifecycleState) String() string {
	switch s {
	case policyLifecycleUnknown:
		return "unknown"
	case policyLifecycleRecovering:
		return "recovering"
	case policyLifecycleReady:
		return "ready"
	case policyLifecycleBlocked:
		return "blocked"
	default:
		return "unknown"
	}
}

func (p *DynamicPolicy) requireReady() error {
	p.RLock()
	defer p.RUnlock()
	return p.requireReadyLocked()
}

func (p *DynamicPolicy) requireReadyLocked() error {
	if p.lifecycleState == policyLifecycleReady {
		return nil
	}
	if p.lifecycleErr != nil {
		return fmt.Errorf("cpu policy is %s: %w", p.lifecycleState.String(), p.lifecycleErr)
	}
	return fmt.Errorf("cpu policy is %s", p.lifecycleState.String())
}

func (p *DynamicPolicy) lockReadyForMutation() error {
	p.Lock()
	if err := p.requireReadyLocked(); err != nil {
		p.Unlock()
		return err
	}
	return nil
}

func (p *DynamicPolicy) recoverCommittedTarget(ctx context.Context) error {
	target, err := p.state.PrepareDurableTarget()
	if err != nil {
		return fmt.Errorf("prepare committed target for bootstrap recovery: %w", err)
	}

	materializationTarget, err := BuildMaterializationTarget(
		target, p.cpuTopology(), p.effectiveReclaimOverlapForTarget(target))
	if err != nil {
		return fmt.Errorf("build committed target for bootstrap recovery: %w", err)
	}
	const maxBootstrapConvergenceAttempts = 3
	for attempt := 1; attempt <= maxBootstrapConvergenceAttempts; attempt++ {
		err = p.materialize(ctx, materializationTarget)
		if err == nil {
			break
		}
		if !errors.Is(err, cpusetmaterializer.ErrCPUSetNotConverged) || attempt == maxBootstrapConvergenceAttempts {
			return fmt.Errorf("converge committed target during bootstrap recovery after %d attempt(s): %w", attempt, err)
		}
	}
	return nil
}

func (p *DynamicPolicy) runBulkheadPeriodicalHandlers(
	coreConf *config.Configuration,
	extraConf interface{},
	dynamicConf *dynamicconfig.DynamicAgentConfiguration,
	emitter metrics.MetricEmitter,
	metaServer *metaserver.MetaServer,
) {
	if err := p.lockReadyForMutation(); err != nil {
		return
	}
	defer p.Unlock()
	if runner, ok := p.cpuSetMaterializer.(interface{ RunPeriodicalHandlers() }); ok {
		runner.RunPeriodicalHandlers()
	}
}
