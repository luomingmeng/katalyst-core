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

package memory

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	cliflag "k8s.io/component-base/cli/flag"

	configapi "github.com/kubewharf/katalyst-api/pkg/apis/config/v1alpha1"
	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	pluginapi "github.com/kubewharf/katalyst-api/pkg/protocol/evictionplugin/v1alpha1"
	evictionoptions "github.com/kubewharf/katalyst-core/cmd/katalyst-agent/app/options/dynamic/adminqos/eviction"
	evictionconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/adminqos/eviction"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/crd"
	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
	"github.com/kubewharf/katalyst-core/pkg/util/native"
)

func makeHelper(metaServer *metaserver.MetaServer) (*EvictionHelper, error) {
	conf := makeConf()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 1, 2)
	if err != nil {
		return nil, err
	}

	metaServer.KatalystMachineInfo = &machine.KatalystMachineInfo{
		CPUTopology: cpuTopology,
	}
	metaServer.MetricsFetcher = metric.NewFakeMetricsFetcher(metrics.DummyMetrics{})

	return NewEvictionHelper(&metrics.DummyMetrics{}, metaServer, conf), nil
}

func TestEvictionHelper_getEvictionCmpFuncs(t *testing.T) {
	t.Parallel()

	metaServer := makeMetaServer()
	helper, err := makeHelper(metaServer)
	assert.NoError(t, err)
	assert.NotNil(t, helper)

	conf := makeConf()

	fakeMetricsFetcher := metaServer.MetricsFetcher.(*metric.FakeMetricsFetcher)
	assert.NotNil(t, fakeMetricsFetcher)

	pods := []*v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "000001",
				Name: "pod-1",
				Annotations: map[string]string{
					apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelReclaimedCores,
				},
			},
			Spec: v1.PodSpec{
				Priority: &lowPriority,
				Containers: []v1.Container{
					{
						Name: "c",
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "000002",
				Name: "pod-2",
				Annotations: map[string]string{
					apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelReclaimedCores,
				},
			},
			Spec: v1.PodSpec{
				Priority: &lowPriority,
				Containers: []v1.Container{
					{
						Name: "c",
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "000003",
				Name: "pod-3",
				Annotations: map[string]string{
					apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelReclaimedCores,
				},
			},
			Spec: v1.PodSpec{
				Priority: &highPriority,
				Containers: []v1.Container{
					{
						Name: "c",
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "000004",
				Name: "pod-4",
				Annotations: map[string]string{
					apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelReclaimedCores,
				},
			},
			Spec: v1.PodSpec{
				Priority: &highPriority,
				Containers: []v1.Container{
					{
						Name: "c",
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "000005",
				Name: "pod-5",
				Annotations: map[string]string{
					apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
				},
			},
			Spec: v1.PodSpec{
				Priority: &lowPriority,
				Containers: []v1.Container{
					{
						Name: "c",
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "000006",
				Name: "pod-6",
				Annotations: map[string]string{
					apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
				},
			},
			Spec: v1.PodSpec{
				Priority: &lowPriority,
				Containers: []v1.Container{
					{
						Name: "c",
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "000007",
				Name: "pod-7",
				Annotations: map[string]string{
					apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
				},
			},
			Spec: v1.PodSpec{
				Priority: &highPriority,
				Containers: []v1.Container{
					{
						Name: "c",
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "000008",
				Name: "pod-8",
				Annotations: map[string]string{
					apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
				},
			},
			Spec: v1.PodSpec{
				Priority: &highPriority,
				Containers: []v1.Container{
					{
						Name: "c",
					},
				},
			},
		},
	}

	podUsageSystem := []float64{
		5 * 1024 * 1024 * 1024,
		10 * 1024 * 1024 * 1024,
		20 * 1024 * 1024 * 1024,
		30 * 1024 * 1024 * 1024,
		40 * 1024 * 1024 * 1024,
		50 * 1024 * 1024 * 1024,
		60 * 1024 * 1024 * 1024,
		70 * 1024 * 1024 * 1024,
		80 * 1024 * 1024 * 1024,
	}

	now := time.Now()
	for i, pod := range pods {
		fakeMetricsFetcher.SetContainerMetric(string(pod.UID), pod.Spec.Containers[0].Name, consts.MetricMemUsageContainer, utilmetric.MetricData{Value: podUsageSystem[i], Time: &now})
	}
	general.NewMultiSorter(helper.getEvictionCmpFuncs(conf.GetDynamicConfiguration().SystemEvictionRankingMetrics,
		nonExistNumaID)...).Sort(native.NewPodSourceImpList(pods))

	wantPodNameList := []string{
		"pod-2",
		"pod-1",
		"pod-4",
		"pod-3",
		"pod-6",
		"pod-5",
		"pod-8",
		"pod-7",
	}
	for i := range pods {
		assert.Equal(t, wantPodNameList[i], pods[i].Name)
	}
}

func TestEvictionHelper_getEvictionCmpFuncs_SaleMode(t *testing.T) {
	t.Parallel()

	metaServer := makeMetaServer()
	helper, err := makeHelper(metaServer)
	assert.NoError(t, err)
	assert.NotNil(t, helper)

	pods := []*v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "000001",
				Name: "reserved",
				Annotations: map[string]string{
					apiconsts.PodAnnotationSaleModeKey: apiconsts.PodSaleModeReserved,
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:  "000002",
				Name: "spot",
				Annotations: map[string]string{
					apiconsts.PodAnnotationSaleModeKey: apiconsts.PodSaleModeSpot,
				},
			},
		},
	}

	general.NewMultiSorter(helper.getEvictionCmpFuncs([]string{evictionconfig.FakeMetricSaleMode}, nonExistNumaID)...).
		Sort(native.NewPodSourceImpList(pods))

	assert.Equal(t, []string{"spot", "reserved"}, []string{pods[0].Name, pods[1].Name})
}

func nativeQoSTestPod(name, katalystQoS string, nativeQoS v1.PodQOSClass, priority int32) *v1.Pod {
	resources := v1.ResourceRequirements{}
	if nativeQoS != v1.PodQOSBestEffort {
		resources.Requests = v1.ResourceList{v1.ResourceCPU: resource.MustParse("1"), v1.ResourceMemory: resource.MustParse("1Gi")}
		resources.Limits = v1.ResourceList{v1.ResourceCPU: resource.MustParse("2"), v1.ResourceMemory: resource.MustParse("2Gi")}
		if nativeQoS == v1.PodQOSGuaranteed {
			resources.Limits = resources.Requests.DeepCopy()
		}
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name), Annotations: map[string]string{apiconsts.PodAnnotationQoSLevelKey: katalystQoS}},
		Spec:       v1.PodSpec{Priority: &priority, Containers: []v1.Container{{Name: "c", Resources: resources}}},
	}
}

func TestEvictionHelperNativeQoS(t *testing.T) {
	t.Parallel()
	h, err := makeHelper(makeMetaServer())
	require.NoError(t, err)
	guaranteed := nativeQoSTestPod("guaranteed", apiconsts.PodAnnotationQoSLevelReclaimedCores, v1.PodQOSGuaranteed, 0)
	burstable := nativeQoSTestPod("burstable", apiconsts.PodAnnotationQoSLevelReclaimedCores, v1.PodQOSBurstable, 0)
	bestEffort := nativeQoSTestPod("best-effort", apiconsts.PodAnnotationQoSLevelSharedCores, v1.PodQOSBestEffort, 100)
	// Status is not authoritative for projected Sandbox objects.
	bestEffort.Status.QOSClass = v1.PodQOSGuaranteed
	initBurstable := guaranteed.DeepCopy()
	initBurstable.Name = "init-burstable"
	initBurstable.Spec.InitContainers = []v1.Container{{Name: "init"}}
	multiBurstable := guaranteed.DeepCopy()
	multiBurstable.Name = "multi-burstable"
	multiBurstable.Spec.Containers = append(multiBurstable.Spec.Containers, v1.Container{Name: "sidecar"})

	// Native QoS and priority sorting must not read cgroup metrics.
	h.metaServer.MetricsFetcher = nil
	cmp := h.getEvictionCmpFuncs([]string{"native.qos.pod"}, nonExistNumaID)[0]
	for _, pair := range [][2]*v1.Pod{{bestEffort, burstable}, {burstable, guaranteed}, {bestEffort, guaranteed}, {initBurstable, guaranteed}, {multiBurstable, guaranteed}, {guaranteed, {}}, {guaranteed, nil}} {
		assert.Less(t, cmp(pair[0], pair[1]), 0)
		assert.Greater(t, cmp(pair[1], pair[0]), 0)
	}
	for _, pod := range []*v1.Pod{bestEffort, burstable, guaranteed} {
		assert.Zero(t, cmp(pod, pod.DeepCopy()))
	}
	assert.Zero(t, cmp(initBurstable, burstable))
	assert.Zero(t, cmp(multiBurstable, burstable))
	assert.Less(t, h.getEvictionCmpFuncs([]string{"native.qos.pod"}, 0)[0](bestEffort, guaranteed), 0)

	eligible := h.filterPods([]*v1.Pod{guaranteed, burstable, bestEffort}, actionReclaimedEviction)
	require.Len(t, eligible, 2)
	assert.Equal(t, "burstable", native.NewPodSourceList(eligible).Sort(cmp).TopN(1)[0].Name)

	tests := []struct {
		name    string
		metrics []string
		want    []string
	}{
		{"native-first", []string{"native.qos.pod", evictionconfig.FakeMetricPriority}, []string{"best-effort", "burstable", "guaranteed"}},
		{"priority-first", []string{evictionconfig.FakeMetricPriority, "native.qos.pod"}, []string{"burstable", "guaranteed", "best-effort"}},
		{"katalyst-first", []string{evictionconfig.FakeMetricQoSLevel, "native.qos.pod"}, []string{"burstable", "guaranteed", "best-effort"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pods := []*v1.Pod{guaranteed, burstable, bestEffort}
			sorted := native.NewPodSourceList(pods).Sort(h.getEvictionCmpFuncs(tt.metrics, nonExistNumaID)...).Pods()
			names := make([]string, len(sorted))
			for i, pod := range sorted {
				names[i] = pod.Name
			}
			assert.Equal(t, tt.want, names)
			assert.Equal(t, guaranteed, pods[0])
		})
	}

	// Equal native QoS classes continue through sale mode and memory usage.
	f := metric.NewFakeMetricsFetcher(metrics.DummyMetrics{}).(*metric.FakeMetricsFetcher)
	h.metaServer.MetricsFetcher = f
	spotLow, spotHigh, reserved := guaranteed.DeepCopy(), guaranteed.DeepCopy(), guaranteed.DeepCopy()
	spotLow.Name, spotLow.UID = "spot-low", "spot-low"
	spotHigh.Name, spotHigh.UID = "spot-high", "spot-high"
	reserved.Name, reserved.UID = "reserved", "reserved"
	spotLow.Annotations[apiconsts.PodAnnotationSaleModeKey] = apiconsts.PodSaleModeSpot
	spotHigh.Annotations[apiconsts.PodAnnotationSaleModeKey] = apiconsts.PodSaleModeSpot
	reserved.Annotations[apiconsts.PodAnnotationSaleModeKey] = apiconsts.PodSaleModeReserved
	bestEffort.Spec.Priority = guaranteed.Spec.Priority
	bestEffort.Annotations[apiconsts.PodAnnotationQoSLevelKey] = apiconsts.PodAnnotationQoSLevelReclaimedCores
	bestEffort.Annotations[apiconsts.PodAnnotationSaleModeKey] = apiconsts.PodSaleModeReserved
	now := time.Now()
	for _, sample := range []struct {
		pod   *v1.Pod
		usage float64
	}{{spotLow, 10}, {spotHigh, 20}, {reserved, 100}, {bestEffort, 1}} {
		f.SetContainerMetric(string(sample.pod.UID), "c", consts.MetricMemUsageContainer, utilmetric.MetricData{Value: sample.usage, Time: &now})
		f.SetContainerNumaMetric(string(sample.pod.UID), "c", 0, consts.MetricsMemTotalPerNumaContainer, utilmetric.MetricData{Value: sample.usage, Time: &now})
	}
	for _, numaID := range []int{nonExistNumaID, 0} {
		metricName := consts.MetricMemUsageContainer
		if numaID >= 0 {
			metricName = consts.MetricsMemTotalPerNumaContainer
		}
		ranking := []string{evictionconfig.FakeMetricQoSLevel, evictionconfig.FakeMetricPriority, "native.qos.pod", evictionconfig.FakeMetricSaleMode, metricName}
		sorted := native.NewPodSourceList([]*v1.Pod{reserved, spotLow, spotHigh, bestEffort}).Sort(h.getEvictionCmpFuncs(ranking, numaID)...).Pods()
		assert.Equal(t, []*v1.Pod{bestEffort, spotHigh, spotLow, reserved}, sorted)
	}
}

func TestMemoryPluginsNativeQoSFromConfiguration(t *testing.T) {
	t.Parallel()
	for _, source := range []string{"flags", "aqc"} {
		for _, pluginType := range []string{"system", "numa"} {
			t.Run(source+"/"+pluginType, func(t *testing.T) {
				conf := makeConf()
				if source == "flags" {
					opts := evictionoptions.NewMemoryPressureEvictionOptions()
					flags := &cliflag.NamedFlagSets{}
					opts.AddFlags(flags)
					require.NoError(t, flags.FlagSet("eviction-memory-pressure").Parse([]string{"--eviction-system-ranking-metrics=native.qos.pod", "--eviction-numa-ranking-metrics=native.qos.pod"}))
					require.NoError(t, opts.ApplyTo(conf.GetDynamicConfiguration().MemoryPressureEvictionConfiguration))
				} else {
					conf.GetDynamicConfiguration().MemoryPressureEvictionConfiguration.ApplyConfiguration(&crd.DynamicConfigCRD{
						AdminQoSConfiguration: &configapi.AdminQoSConfiguration{Spec: configapi.AdminQoSConfigurationSpec{Config: configapi.AdminQoSConfig{
							EvictionConfig: &configapi.EvictionConfig{MemoryPressureEvictionConfig: &configapi.MemoryPressureEvictionConfig{
								SystemEvictionRankingMetrics: []configapi.SystemEvictionRankingMetric{"native.qos.pod"},
								NumaEvictionRankingMetrics:   []configapi.NumaEvictionRankingMetric{"native.qos.pod"},
							}},
						}}},
					})
				}
				pods := []*v1.Pod{
					nativeQoSTestPod("guaranteed", apiconsts.PodAnnotationQoSLevelReclaimedCores, v1.PodQOSGuaranteed, 0),
					nativeQoSTestPod("burstable", apiconsts.PodAnnotationQoSLevelReclaimedCores, v1.PodQOSBurstable, 0),
					nativeQoSTestPod("best-effort", apiconsts.PodAnnotationQoSLevelReclaimedCores, v1.PodQOSBestEffort, 0),
				}
				request := &pluginapi.GetTopEvictionPodsRequest{ActivePods: pods, TopN: 1}
				var resp *pluginapi.GetTopEvictionPodsResponse
				var err error
				if pluginType == "system" {
					p, e := makeSystemPressureEvictionPlugin(conf)
					require.NoError(t, e)
					p.isUnderSystemPressure, p.systemAction = true, actionReclaimedEviction
					resp, err = p.GetTopEvictionPods(context.Background(), request)
				} else {
					p, e := makeNumaPressureEvictionPlugin(conf)
					require.NoError(t, e)
					p.isUnderNumaPressure = true
					p.numaActionMap = map[int]int{0: actionReclaimedEviction}
					f := p.metaServer.MetricsFetcher.(*metric.FakeMetricsFetcher)
					now := time.Now()
					f.SetNumaMetric(0, consts.MetricMemTotalNuma, utilmetric.MetricData{Value: 100, Time: &now})
					for _, pod := range pods {
						f.SetContainerNumaMetric(string(pod.UID), "c", 0, consts.MetricsMemTotalPerNumaContainer, utilmetric.MetricData{Value: 10, Time: &now})
					}
					resp, err = p.GetTopEvictionPods(context.Background(), request)
				}
				require.NoError(t, err)
				require.Len(t, resp.TargetPods, 1)
				assert.Equal(t, "best-effort", resp.TargetPods[0].Name)
			})
		}
	}
}
