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

package staticpolicy

import (
	"reflect"
	"time"

	gpuconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/state"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

// startTopologyReconcilerLocked is called once per Start while holding the policy lock.
func (p *StaticPolicy) startTopologyReconcilerLocked() {
	p.topologyNotifierOnce.Do(func() {
		p.topologyNotifyCh = make(chan struct{}, 1)
		// Registry callbacks run synchronously on the updater's goroutine.
		// Keep them non-blocking; state reconciliation belongs to the worker.
		p.DeviceTopologyRegistry.RegisterTopologyChangeNotifier(func() {
			select {
			case p.topologyNotifyCh <- struct{}{}:
			default:
			}
		})
	})
	done := make(chan struct{})
	p.topologyReconcileDone = done
	go p.runTopologyReconciler(p.stopCh, p.topologyNotifyCh, done, gpuconsts.StateCheckPeriod)
}

func (p *StaticPolicy) runTopologyReconciler(stop <-chan struct{}, notify <-chan struct{}, done chan<- struct{}, interval time.Duration) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	p.reconcileDeviceStateLoop(stop, notify, ticker.C)
}

func (p *StaticPolicy) reconcileDeviceStateLoop(stop, notify <-chan struct{}, ticks <-chan time.Time) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		if err := p.reconcileDeviceState(); err != nil {
			general.ErrorS(err, "reconcile GPU device state failed")
		}
		select {
		case <-stop:
			return
		case <-notify:
		case <-ticks:
		}
	}
}

// reconcileDeviceState merges kubelet ownership before new device capacity. The
// same policy lock used by allocation/removal protects the read/merge/write sequence.
func (p *StaticPolicy) reconcileDeviceState() error {
	p.Lock()
	defer p.Unlock()
	return p.reconcileDeviceStateLocked()
}

// Allocation invokes the same ownership barrier under the policy lock. Other
// rebuild paths may have discovered an ID before the background worker runs.
func (p *StaticPolicy) reconcileDeviceStateLocked() error {

	// Do not initialize an empty GPU store while device discovery is pending.
	topology, err := state.GetMergedDeviceTopology(p.DeviceTopologyRegistry, p.Conf.GPUDeviceNames)
	if p.GetState() == nil && (err != nil || topology == nil || len(topology.Devices) == 0) {
		return nil
	}
	if p.GetState() == nil {
		if err := p.InitState(); err != nil {
			return err
		}
	}

	s := p.GetState()
	entries, ownershipChanged, err := p.MergeKubeletGPUAllocationEntries(s)
	if err != nil {
		return err
	}
	current := s.GetMachineState()
	if ownershipChanged {
		// Only GPU ownership changed. Preserve initialized optional resources
		// (e.g. an idle RDMA map) rather than resetting the whole machine state.
		gpuState, generateErr := p.GenerateResourceStateFromPodEntries(gpuconsts.GPUDeviceType, entries[gpuconsts.GPUDeviceType])
		if generateErr != nil {
			return generateErr
		}
		current[gpuconsts.GPUDeviceType] = gpuState
	}
	candidate, err := state.ReconcileMachineState(current, entries, p.DefaultResourceStateGeneratorRegistry)
	if err != nil {
		return err
	}
	if ownershipChanged || !reflect.DeepEqual(candidate, s.GetMachineState()) {
		// Ownership is committed before the state notifier can expose capacity.
		if ownershipChanged {
			s.SetPodResourceEntries(entries, false)
		}
		s.SetMachineState(candidate, false)
		p.persistPending = true
	}
	if p.persistPending {
		return p.persistDeviceStateLocked()
	}
	return nil
}

func (p *StaticPolicy) persistDeviceStateLocked() error {
	if err := p.GetState().StoreState(); err != nil {
		return err
	}
	p.persistPending = false
	return nil
}
