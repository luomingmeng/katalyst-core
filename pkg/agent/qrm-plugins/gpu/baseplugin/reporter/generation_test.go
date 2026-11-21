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

package reporter

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	nodeapi "github.com/kubewharf/katalyst-api/pkg/apis/node/v1alpha1"
	apiConsts "github.com/kubewharf/katalyst-api/pkg/consts"
	reportapi "github.com/kubewharf/katalyst-api/pkg/protocol/reporterplugin/v1alpha1"
	gpuconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/state"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// Both RPCs must reject a build whose captured state was invalidated before
// publication. Channels control the interleaving; no timing-dependent sleeps.
func TestReportBuildInvalidatedDuringReconcile(t *testing.T) {
	for _, watch := range []bool{false, true} {
		name := "unary"
		if watch {
			name = "watch"
		}
		t.Run(name, func(t *testing.T) {
			r := machine.NewDeviceTopologyRegistry(metrics.DummyMetrics{})
			r.RegisterDeviceTopologyProvider("gpu", machine.NewDeviceTopologyProvider())
			require.NoError(t, r.SetDeviceTopology("gpu", &machine.DeviceTopology{Devices: map[string]machine.DeviceInfo{"gpu-0": {Health: "Healthy", NumaNodes: []int{0}}, "gpu-1": {Health: "Healthy", NumaNodes: []int{0}}}}))
			reg := state.NewDefaultResourceStateGeneratorRegistry()
			for _, resource := range []string{gpuconsts.GPUDeviceType, string(apiConsts.ResourceGPUMemory), string(apiConsts.ResourceMilliGPU)} {
				reg.RegisterResourceStateGenerator(resource, state.NewGenericDefaultResourceStateGenerator([]string{"gpu"}, r, 1, true))
			}
			s, err := state.NewGPUPluginState(nil, reg)
			require.NoError(t, err)
			stale := s.GetMachineState()
			for _, am := range stale {
				delete(am, "gpu-1")
			}
			s.SetMachineState(stale, false)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			p := &gpuReporterPlugin{ctx: ctx, deviceTopologyRegistry: r, gpuDeviceNames: []string{"gpu"}, deviceTypeToNames: map[string]sets.String{gpuconsts.GPUDeviceType: sets.NewString("gpu")}, numaSocketZoneNodeMap: map[util.ZoneNode]util.ZoneNode{util.GenerateNumaZoneNode(0): util.GenerateSocketZoneNode(0)}, reportNotifyCh: make(chan struct{}, 1), reportRetryInterval: time.Millisecond}
			captured, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unlock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unlock)
			gate := &reportSnapshotGate{State: s, captured: captured, release: release}
			p.stateGetter = func() state.State { return gate }
			responses := make(chan *reportapi.GetReportContentResponse, 2)
			done := make(chan error, 1)
			if watch {
				server := &mockReporterPluginServer{ctx: ctx, sendCh: responses}
				go func() { done <- p.ListAndWatchReportContent(&reportapi.Empty{}, server) }()
			} else {
				go func() { resp, err := p.GetReportContent(ctx, &reportapi.Empty{}); responses <- resp; done <- err }()
			}
			t.Cleanup(func() {
				unlock()
				cancel()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("report RPC did not stop")
				}
			})
			select {
			case <-captured:
			case <-time.After(time.Second):
				t.Fatal("build did not capture stale state")
			}
			candidate, err := state.ReconcileMachineState(s.GetMachineState(), s.GetPodResourceEntries(), reg)
			require.NoError(t, err)
			s.SetMachineState(candidate, false)
			p.Trigger()
			unlock()
			var resp *reportapi.GetReportContentResponse
			select {
			case resp = <-responses:
			case <-time.After(time.Second):
				t.Fatal("no reconciled report")
			}
			require.NotNil(t, resp)
			assertReportGPUCapacity(t, resp, "gpu-1")
			again, err := p.GetReportContent(ctx, &reportapi.Empty{})
			require.NoError(t, err)
			require.Equal(t, resp, again)
			assertReportGPUCapacity(t, again, "gpu-1")
		})
	}
}

type reportSnapshotGate struct {
	state.State
	once     sync.Once
	captured chan struct{}
	release  <-chan struct{}
}

func (s *reportSnapshotGate) GetMachineState() state.AllocationResourcesMap {
	snapshot := s.State.GetMachineState()
	s.once.Do(func() { close(s.captured); <-s.release })
	return snapshot
}
func assertReportGPUCapacity(t *testing.T, resp *reportapi.GetReportContentResponse, id string) {
	t.Helper()
	var zones []*nodeapi.TopologyZone
	require.NoError(t, json.Unmarshal(resp.Content[0].Field[0].Value, &zones))
	found := false
	var visit func([]*nodeapi.TopologyZone)
	visit = func(zs []*nodeapi.TopologyZone) {
		for _, z := range zs {
			if z.Name == id {
				found = true
				require.NotNil(t, z.Resources.Allocatable)
				for _, name := range []string{"gpu", string(apiConsts.ResourceGPUMemory), string(apiConsts.ResourceMilliGPU)} {
					q := (*z.Resources.Allocatable)[v1.ResourceName(name)]
					require.Equal(t, int64(1), q.Value())
				}
			}
			visit(z.Children)
		}
	}
	visit(zones)
	require.True(t, found)
}
