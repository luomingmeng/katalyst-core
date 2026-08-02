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
	"testing"
	"time"

	cpusetutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/util"
	"github.com/kubewharf/katalyst-core/pkg/config"
	bulkheadconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/bulkhead"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestCPUSetAdjustmentHandlerTimeoutCoversTopologyConvergenceBudget(t *testing.T) {
	t.Parallel()

	conf := config.NewConfiguration()
	if cpuSetAdjustmentHandlerTimeout(conf) <= bulkheadconfig.DefaultTopologyConvergenceDeadline {
		t.Fatalf("outer cpuset adjustment timeout %s must exceed topology convergence budget %s",
			cpuSetAdjustmentHandlerTimeout(conf), bulkheadconfig.DefaultTopologyConvergenceDeadline)
	}
}

func TestCPUSetAdjustmentHandlerTimeoutDerivesFromConfiguredTopologyDeadline(t *testing.T) {
	t.Parallel()

	conf := config.NewConfiguration()
	conf.CPUQRMPluginConfig.BulkheadConfiguration.TopologyConvergenceBudget.DeadlineDuration = 750 * time.Millisecond
	got := cpuSetAdjustmentHandlerTimeout(conf)
	if got <= 750*time.Millisecond || got >= 15*time.Second {
		t.Fatalf("derived outer timeout = %s, want bounded margin above configured 750ms", got)
	}
}

func TestRunCPUSetAdjustmentHandlersDoesNotHoldPolicyLockDuringExecution(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	p := &DynamicPolicy{
		cpuSetAdjustmentHandlers: map[string]cpusetutil.CPUSetAdjustmentHandler{
			"blocking-io": func(context.Context, cpusetutil.CPUSetAdjustmentHandlerCtx) error {
				close(started)
				<-release
				return nil
			},
		},
	}

	runDone := make(chan error, 1)
	go func() {
		p.Lock()
		defer p.Unlock()
		runDone <- p.runCPUSetAdjustmentHandlers(context.Background())
	}()
	<-started

	lockAcquired := make(chan struct{})
	go func() {
		p.Lock()
		close(lockAcquired)
		p.Unlock()
	}()

	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		close(release)
		<-runDone
		t.Fatal("DynamicPolicy lock remained held while adjustment handler executed")
	}
	close(release)
	if err := <-runDone; err != nil {
		t.Fatalf("runCPUSetAdjustmentHandlers() error = %v", err)
	}
}

func TestRunCPUSetAdjustmentHandlersFenceRejectsStateMutationWithoutNewRound(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	committed := make(chan bool, 1)
	topology, err := machine.GenerateDummyCPUTopology(8, 1, 4)
	if err != nil {
		t.Fatalf("GenerateDummyCPUTopology() error = %v", err)
	}
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	if err != nil {
		t.Fatalf("getTestDynamicPolicyWithInitialization() error = %v", err)
	}
	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"blocking-io": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			close(started)
			<-release
			committed <- in.CommitIfGenerationCurrent(in.Generation, func() {})
			return nil
		},
	}

	runDone := make(chan error, 1)
	go func() {
		p.Lock()
		defer p.Unlock()
		runDone <- p.runCPUSetAdjustmentHandlers(context.Background())
	}()
	<-started

	p.Lock()
	p.state.SetAllowSharedCoresOverlapReclaimedCores(true, false)
	p.Unlock()
	close(release)

	if err := <-runDone; err != nil {
		t.Fatalf("runCPUSetAdjustmentHandlers() error = %v", err)
	}
	if <-committed {
		t.Fatal("generation fence accepted a result calculated from stale policy state")
	}
}

func TestRunCPUSetAdjustmentHandlersSerializesLockFreeRounds(t *testing.T) {
	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	secondStarted := make(chan struct{})
	p := &DynamicPolicy{}
	p.cpuSetAdjustmentHandlers = map[string]cpusetutil.CPUSetAdjustmentHandler{
		"blocking-io": func(_ context.Context, in cpusetutil.CPUSetAdjustmentHandlerCtx) error {
			if in.Generation == 1 {
				close(firstStarted)
				<-firstRelease
			} else {
				close(secondStarted)
			}
			return nil
		},
	}

	run := func(done chan<- error) {
		p.Lock()
		defer p.Unlock()
		done <- p.runCPUSetAdjustmentHandlers(context.Background())
	}
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go run(firstDone)
	<-firstStarted
	go run(secondDone)

	select {
	case <-secondStarted:
		close(firstRelease)
		t.Fatal("second adjustment round executed concurrently with the first")
	case <-time.After(100 * time.Millisecond):
	}
	close(firstRelease)
	if err := <-firstDone; err != nil {
		t.Fatalf("first run error = %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second run error = %v", err)
	}
}
