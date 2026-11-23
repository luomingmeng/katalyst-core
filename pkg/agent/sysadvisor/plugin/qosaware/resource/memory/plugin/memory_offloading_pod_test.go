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

package plugin

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/memory/dynamicpolicy/memoryadvisor"
	sysadvisortypes "github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric"
	metrictypes "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	corepod "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/spd"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
	"github.com/kubewharf/katalyst-core/pkg/util/native"
)

type offloadingPodFetcher struct {
	corepod.PodFetcherStub
	target       metrictypes.PodMetricTarget
	handled      bool
	resolveErr   error
	resolvedUIDs []k8stypes.UID
}

func (f *offloadingPodFetcher) GetPodMetricTargets(context.Context) ([]metrictypes.PodMetricTarget, error) {
	return []metrictypes.PodMetricTarget{f.target}, nil
}

func (f *offloadingPodFetcher) ResolvePodMetricTarget(_ context.Context, pod *v1.Pod) (metrictypes.PodMetricTarget, bool, error) {
	f.resolvedUIDs = append(f.resolvedUIDs, pod.UID)
	if pod.UID == f.target.Pod.UID {
		return f.target, f.handled, f.resolveErr
	}
	return metrictypes.PodMetricTarget{}, false, nil
}

type offloadingTestEngine struct {
	*tmoEngineInstance
	calculations int
}

func (e *offloadingTestEngine) CalculateOffloadingTargetSize() {
	e.calculations++
	e.offloadingTargetSize = 8 * 1024 * 1024
}

func offloadingTestPod(name string) *v1.Pod {
	// Both sources use the same RuntimeClass: classification must use the
	// source-bound resolver, not the runtime name or the cgroup root.
	runtimeClass := "kata"
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: k8stypes.UID(name), Name: name, Namespace: "default",
			Annotations: map[string]string{apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores},
		},
		Spec: v1.PodSpec{
			RuntimeClassName: &runtimeClass,
			Containers:       []v1.Container{{Name: "main"}, {Name: "sidecar"}},
		},
		Status: v1.PodStatus{
			Phase:             v1.PodRunning,
			ContainerStatuses: []v1.ContainerStatus{{Name: "main"}, {Name: "sidecar"}},
		},
	}
}

func TestTransparentMemoryOffloadingReconcileExcludesWholePodTargets(t *testing.T) {
	for _, tc := range []struct {
		name       string
		path       string
		handled    bool
		resolveErr error
		seed       bool
	}{
		{name: "separate-root", path: "/sandboxes/instance", handled: true, seed: true},
		{name: "kubernetes-root", path: "/kubepods/burstable/instance", handled: true, seed: true},
		{name: "unavailable-mapping", handled: true, resolveErr: errors.New("inventory unavailable"), seed: true},
		{name: "unclassified-error", resolveErr: errors.New("source resolution failed"), seed: true},
		{name: "new-target", path: "/kubepods/burstable/instance", handled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ordinary, wholePod := offloadingTestPod("ordinary"), offloadingTestPod("whole-pod")
			fetcher := &offloadingPodFetcher{
				PodFetcherStub: corepod.PodFetcherStub{PodList: []*v1.Pod{ordinary, wholePod}},
				target:         metrictypes.PodMetricTarget{Pod: wholePod, CgroupPath: tc.path},
				handled:        tc.handled, resolveErr: tc.resolveErr,
			}
			tmo, ordinaryEngines := offloadingTestPlugin(fetcher, ordinary)
			var excludedEngines []*offloadingTestEngine
			if tc.seed {
				excludedEngines = seedOffloadingTestEngines(tmo, wholePod)
			}

			require.NoError(t, tmo.Reconcile(nil))
			require.Len(t, tmo.containerTmoEngines, len(ordinary.Status.ContainerStatuses))
			require.ElementsMatch(t, []k8stypes.UID{ordinary.UID, wholePod.UID}, fetcher.resolvedUIDs)
			for _, engine := range excludedEngines {
				require.Zero(t, engine.calculations, "excluded targets must not run the offloading calculation")
			}
			for _, engine := range ordinaryEngines {
				require.Equal(t, 1, engine.calculations)
			}
			assertOrdinaryOffloadingAdvices(t, tmo.GetAdvices(), ordinary)
		})
	}
}

func TestTransparentMemoryOffloadingReconcileCalculatesOrdinaryPodAdvice(t *testing.T) {
	ordinary, wholePod := offloadingTestPod("ordinary"), offloadingTestPod("whole-pod")
	fetcher := &offloadingPodFetcher{
		PodFetcherStub: corepod.PodFetcherStub{PodList: []*v1.Pod{ordinary, wholePod}},
		target:         metrictypes.PodMetricTarget{Pod: wholePod, CgroupPath: "/kubepods/burstable/instance"},
		handled:        true,
	}
	tmo, _ := offloadingTestPlugin(fetcher, ordinary)
	// Exercise normal engine construction and the real PSI policy, not the
	// seeded test engine used to check stale-state cleanup above.
	tmo.containerTmoEngines = make(map[consts.PodContainerName]TmoEngine)
	tmo.emitter = metrics.DummyMetrics{}
	fakeMetrics := tmo.metaServer.MetricsFetcher.(*metric.FakeMetricsFetcher)
	now := time.Now()
	fakeMetrics.RegisterExternalMetric(func(store *utilmetric.MetricStore) {
		for _, status := range ordinary.Status.ContainerStatuses {
			for metricName, value := range map[string]float64{
				consts.MetricMemPsiAvg60Container:           0,
				consts.MetricMemPgstealContainer:            0,
				consts.MetricMemPgscanContainer:             0,
				consts.MetricMemWorkingsetRefaultContainer:  0,
				consts.MetricMemWorkingsetActivateContainer: 0,
				consts.MetricMemUsageContainer:              100 * 1024 * 1024,
				consts.MetricMemInactiveAnonContainer:       10 * 1024 * 1024,
				consts.MetricMemInactiveFileContainer:       10 * 1024 * 1024,
				consts.MetricMemCacheContainer:              50 * 1024 * 1024,
				consts.MetricMemMappedContainer:             0,
			} {
				store.SetContainerMetric(string(ordinary.UID), status.Name, metricName, utilmetric.MetricData{Value: value, Time: &now})
			}
		}
	})
	fakeMetrics.Run(context.Background())
	require.NoError(t, tmo.Reconcile(nil))
	result := tmo.GetAdvices()
	require.Len(t, result.ContainerEntries, 2)
	for _, entry := range result.ContainerEntries {
		require.Equal(t, string(ordinary.UID), entry.PodUID)
		require.Equal(t, consts.ControlKnobON, entry.Values[string(memoryadvisor.ControlKnobKeySwapMax)])
		require.Equal(t, strconv.FormatInt(1024*1024, 10), entry.Values[string(memoryadvisor.ControlKnowKeyMemoryOffloading)])
	}
}

func TestTransparentMemoryOffloadingReconcileWithoutMetricTargetProvider(t *testing.T) {
	ordinary := offloadingTestPod("ordinary")
	fetcher := &corepod.PodFetcherStub{PodList: []*v1.Pod{ordinary}}
	tmo, engines := offloadingTestPlugin(fetcher, ordinary)
	require.NoError(t, tmo.Reconcile(nil))
	for _, engine := range engines {
		require.Equal(t, 1, engine.calculations)
	}
	assertOrdinaryOffloadingAdvices(t, tmo.GetAdvices(), ordinary)
}

func offloadingTestPlugin(fetcher corepod.PodFetcher, ordinary *v1.Pod) (*transparentMemoryOffloading, []*offloadingTestEngine) {
	conf := config.NewConfiguration()
	conf.GetDynamicConfiguration().BlockConfig = nil
	defaults := conf.GetDynamicConfiguration().TransparentMemoryOffloadingConfiguration.DefaultConfigurations
	defaults.DefaultEnableTMO = true
	defaults.DefaultEnableSwap = true
	defaults.DefaultTMOInterval = 0
	metaServer := &metaserver.MetaServer{
		MetaAgent: &agent.MetaAgent{
			PodFetcher:     fetcher,
			MetricsFetcher: metric.NewFakeMetricsFetcher(nil),
		},
		ServiceProfilingManager: &spd.DummyServiceProfilingManager{},
	}
	tmo := NewTransparentMemoryOffloading(conf, nil, nil, metaServer, nil).(*transparentMemoryOffloading)
	tmo.enableDyingMemcgReclaim = false
	return tmo, seedOffloadingTestEngines(tmo, ordinary)
}

func seedOffloadingTestEngines(tmo *transparentMemoryOffloading, pod *v1.Pod) []*offloadingTestEngine {
	var engines []*offloadingTestEngine
	for _, status := range pod.Status.ContainerStatuses {
		info := &sysadvisortypes.ContainerInfo{
			PodUID: string(pod.UID), PodName: pod.Name, ContainerName: status.Name,
			Labels: pod.Labels, Annotations: pod.Annotations,
			QoSLevel: apiconsts.PodAnnotationQoSLevelSharedCores,
		}
		engine := &offloadingTestEngine{tmoEngineInstance: NewTmoEngineInstance(info, tmo.metaServer, tmo.emitter,
			tmo.conf.GetDynamicConfiguration().TransparentMemoryOffloadingConfiguration)}
		// Seed a positive previous result to detect stale advice from an excluded
		// engine, not merely the absence of newly created engines.
		engine.offloadingTargetSize = 8 * 1024 * 1024
		tmo.containerTmoEngines[native.GeneratePodContainerName(pod.Name, status.Name)] = engine
		engines = append(engines, engine)
	}
	return engines
}

func assertOrdinaryOffloadingAdvices(t *testing.T, result sysadvisortypes.InternalMemoryCalculationResult, pod *v1.Pod) {
	t.Helper()
	require.Len(t, result.ContainerEntries, len(pod.Status.ContainerStatuses))
	require.Empty(t, result.ExtraEntries)
	var names []string
	for _, entry := range result.ContainerEntries {
		require.Equal(t, string(pod.UID), entry.PodUID)
		require.Equal(t, consts.ControlKnobON, entry.Values[string(memoryadvisor.ControlKnobKeySwapMax)])
		require.Equal(t, strconv.FormatInt(8*1024*1024, 10), entry.Values[string(memoryadvisor.ControlKnowKeyMemoryOffloading)])
		names = append(names, entry.ContainerName)
	}
	require.ElementsMatch(t, []string{"main", "sidecar"}, names)
}
