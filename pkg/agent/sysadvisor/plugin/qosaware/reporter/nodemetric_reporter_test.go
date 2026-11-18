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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	nodeapis "github.com/kubewharf/katalyst-api/pkg/apis/node/v1alpha1"
	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-api/pkg/plugins/skeleton"
	"github.com/kubewharf/katalyst-api/pkg/protocol/reporterplugin/v1alpha1"
	"github.com/kubewharf/katalyst-core/pkg/agent/resourcemanager/fetcher"
	resourcereporter "github.com/kubewharf/katalyst-core/pkg/agent/resourcemanager/reporter"
	cnrreporter "github.com/kubewharf/katalyst-core/pkg/agent/resourcemanager/reporter/cnr"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/metacache"
	types2 "github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	metricspool "github.com/kubewharf/katalyst-core/pkg/metrics/metrics-pool"
	"github.com/kubewharf/katalyst-core/pkg/util"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
	"github.com/kubewharf/katalyst-core/pkg/util/process"
)

func generateTestMetaCache(t *testing.T, conf *config.Configuration, metricsReader types.MetricsReader, containers ...types2.ContainerInfo) *metacache.MetaCacheImp {
	metaCache, err := metacache.NewMetaCacheImp(conf, metricspool.DummyMetricsEmitterPool{}, metricsReader)
	require.NoError(t, err)
	require.NotNil(t, metaCache)

	for _, container := range containers {
		c := container
		metaCache.AddContainer(container.PodUID, container.ContainerName, &c)
	}

	return metaCache
}

func TestRunNodeMetricReporter(t *testing.T) {
	t.Parallel()

	regDir, ckDir, statDir, err := tmpDirs()
	require.NoError(t, err)
	defer func() {
		os.RemoveAll(regDir)
		os.RemoveAll(ckDir)
		os.RemoveAll(statDir)
	}()

	conf := generateTestConfiguration(t, regDir, ckDir, statDir)
	clientSet := generateTestGenericClientSet(nil, nil)
	metaServer := generateTestMetaServer(clientSet, conf)
	metaReader := generateTestMetaCache(t, conf, metaServer.MetricsFetcher)

	reporter, err := NewNodeMetricsReporter(metrics.DummyMetrics{}, metaServer, metaReader, conf)

	require.NoError(t, err)
	require.NotNil(t, reporter)

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		reporter.Run(ctx)
	}()
	time.Sleep(1 * time.Second)
	cancel()
	wg.Wait()
}

func mustParse(str string) *resource.Quantity {
	q := resource.MustParse(str)
	return &q
}

type unavailableGroupPodFetcher struct {
	pod.PodFetcher
	listCalls int
}

func (f *unavailableGroupPodFetcher) GetPodList(context.Context, func(*corev1.Pod) bool) ([]*corev1.Pod, error) {
	f.listCalls++
	return nil, fmt.Errorf("pod source unavailable")
}

func TestNodeMetricReporterWithoutGroupMetrics(t *testing.T) {
	regDir, ckDir, stateDir, err := tmpDirs()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(regDir)
		_ = os.RemoveAll(ckDir)
		_ = os.RemoveAll(stateDir)
	})
	conf := generateTestConfiguration(t, regDir, ckDir, stateDir)
	conf.NodeMetricReporterConfiguration.DisableGroupMetric = true
	conf.NodeMetricReporterConfiguration.MetricSlidingWindowTime = conf.NodeMetricReporterConfiguration.SyncPeriod
	metaServer := generateTestMetaServer(generateTestGenericClientSet(nil, nil), conf)
	podFetcher := &unavailableGroupPodFetcher{PodFetcher: metaServer.PodFetcher}
	metaServer.PodFetcher = podFetcher

	// A node-only reporter must not need container metadata or a Pod inventory.
	r, err := NewNodeMetricsReporter(metrics.DummyMetrics{}, metaServer, nil, conf)
	require.NoError(t, err)
	p := r.(*nodeMetricsReporterImpl).GenericPlugin.(*skeleton.PluginRegistrationWrapper).GenericPlugin.(*nodeMetricsReporterPlugin)
	fetcher := metaServer.MetricsFetcher.(*metric.FakeMetricsFetcher)
	fetcher.SetNodeMetric(consts.MetricMemUsedSystem, utilmetric.MetricData{Value: 200 << 30})
	fetcher.SetNodeMetric(consts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5})
	for numaID := 0; numaID < metaServer.NumNUMANodes; numaID++ {
		fetcher.SetNumaMetric(numaID, consts.MetricMemUsedNuma, utilmetric.MetricData{Value: 50 << 30})
		fetcher.SetNumaMetric(numaID, consts.MetricMemFilepageNuma, utilmetric.MetricData{Value: 10 << 30})
		fetcher.SetNumaMetric(numaID, consts.MetricCPUUsageNuma, utilmetric.MetricData{Value: 2})
	}
	oldTime := metav1.NewTime(time.Now().Add(-time.Hour))
	p.nodeMetricStatus = &nodeapis.NodeMetricStatus{
		UpdateTime:  oldTime,
		GroupMetric: []nodeapis.GroupMetricInfo{{QoSLevel: apiconsts.PodAnnotationQoSLevelSharedCores}},
	}
	p.updateNodeMetrics()
	require.Zero(t, podFetcher.listCalls)
	resp, err := p.GetReportContent(context.Background(), &v1alpha1.Empty{})
	require.NoError(t, err)
	require.Len(t, resp.Content, 1)
	require.Equal(t, &util.CNRGroupVersionKind, resp.Content[0].GroupVersionKind)
	require.Len(t, resp.Content[0].Field, 1)
	field := resp.Content[0].Field[0]
	require.Equal(t, v1alpha1.FieldType_Status, field.FieldType)
	require.Equal(t, util.CNRFieldNameNodeMetricStatus, field.FieldName)
	var status nodeapis.NodeMetricStatus
	require.NoError(t, json.Unmarshal(field.Value, &status))
	require.Nil(t, status.GroupMetric)
	require.True(t, status.UpdateTime.After(oldTime.Time))
	require.Equal(t, int64(200<<30), status.NodeMetric.GenericUsage.Memory().Value())
	require.Equal(t, int64(8000), status.NodeMetric.GenericUsage.Cpu().MilliValue())
	require.Len(t, status.NodeMetric.NUMAUsage, metaServer.NumNUMANodes)
	for _, numa := range status.NodeMetric.NUMAUsage {
		require.Equal(t, int64(40<<30), numa.Usage.Memory().Value())
		require.Equal(t, int64(2000), numa.Usage.Cpu().MilliValue())
	}
	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(field.Value, &payload))
	require.NotContains(t, payload, "groupMetric")

	// Node sampling errors must not publish a fresh timestamp or partial sample.
	published := p.nodeMetricStatus
	metaServer.MetricsFetcher = metric.NewFakeMetricsFetcher(metrics.DummyMetrics{})
	p.updateNodeMetrics()
	require.Same(t, published, p.nodeMetricStatus)
	require.Zero(t, podFetcher.listCalls)
	metaServer.MetricsFetcher = fetcher

	// Group-enabled behavior still requires a successful Pod source.
	p.disableGroupMetric = false
	p.updateNodeMetrics()
	require.Equal(t, 1, podFetcher.listCalls)
	require.Same(t, published, p.nodeMetricStatus)
}

func TestNodeMetricReportingToCNR(t *testing.T) {
	regDir, ckDir, stateDir, err := tmpDirs()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(regDir)
		_ = os.RemoveAll(ckDir)
		_ = os.RemoveAll(stateDir)
	})
	conf := generateTestConfiguration(t, regDir, ckDir, stateDir)
	conf.NodeMetricReporterConfiguration.DisableGroupMetric = true
	conf.NodeMetricReporterConfiguration.MetricSlidingWindowTime = conf.NodeMetricReporterConfiguration.SyncPeriod
	conf.AgentReporters = []string{util.CNRGroupVersionKind.Kind}
	oldTime := metav1.NewTime(time.Now().Add(-time.Hour))
	original := &nodeapis.CustomNodeResource{
		TypeMeta:   metav1.TypeMeta{APIVersion: util.CNRGroupVersionKind.Group + "/" + util.CNRGroupVersionKind.Version, Kind: util.CNRGroupVersionKind.Kind},
		ObjectMeta: metav1.ObjectMeta{Name: conf.NodeName, Labels: map[string]string{"existing": "retained"}},
		Status: nodeapis.CustomNodeResourceStatus{
			NodeMetricStatus: &nodeapis.NodeMetricStatus{
				UpdateTime:  oldTime,
				GroupMetric: []nodeapis.GroupMetricInfo{{QoSLevel: apiconsts.PodAnnotationQoSLevelSharedCores}},
			},
		},
	}
	clients := generateTestGenericClientSet(nil, []runtime.Object{original})
	metaServer := generateTestMetaServer(clients, conf)
	metricsFetcher := metaServer.MetricsFetcher.(*metric.FakeMetricsFetcher)
	metricsFetcher.SetNodeMetric(consts.MetricMemUsedSystem, utilmetric.MetricData{Value: 200 << 30})
	metricsFetcher.SetNodeMetric(consts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5})
	for numaID := 0; numaID < metaServer.NumNUMANodes; numaID++ {
		metricsFetcher.SetNumaMetric(numaID, consts.MetricMemUsedNuma, utilmetric.MetricData{Value: 50 << 30})
		metricsFetcher.SetNumaMetric(numaID, consts.MetricMemFilepageNuma, utilmetric.MetricData{Value: 10 << 30})
		metricsFetcher.SetNumaMetric(numaID, consts.MetricCPUUsageNuma, utilmetric.MetricData{Value: 2})
	}
	resourcereporter.RegisterReporterInitializer(util.CNRGroupVersionKind, cnrreporter.NewCNRReporter)
	manager, err := resourcereporter.NewReporterManager(clients, metaServer, metrics.DummyMetrics{}, conf)
	require.NoError(t, err)
	pluginManager, err := fetcher.NewReporterPluginManager(manager, metrics.DummyMetrics{}, metaServer, conf)
	require.NoError(t, err)
	r, err := NewNodeMetricsReporter(metrics.DummyMetrics{}, metaServer, nil, conf)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		r.Run(ctx)
	}()
	defer func() {
		cancel()
		<-finished
	}()
	socket := filepath.Join(regDir, nodeMetricsReporterPluginName+".sock")
	conn, err := process.Dial(socket, 5*time.Second)
	require.NoError(t, err)
	_ = conn.Close()
	require.NoError(t, pluginManager.RegisterPlugin(nodeMetricsReporterPluginName, socket, nil))
	managerFinished := make(chan struct{})
	go func() {
		defer close(managerFinished)
		pluginManager.Run(ctx)
	}()
	defer func() {
		cancel()
		pluginManager.DeRegisterPlugin(nodeMetricsReporterPluginName)
		<-managerFinished
	}()

	var observed *nodeapis.CustomNodeResource
	require.Eventually(t, func() bool {
		observed, err = clients.InternalClient.NodeV1alpha1().CustomNodeResources().Get(ctx, conf.NodeName, metav1.GetOptions{})
		if err != nil || observed.Status.NodeMetricStatus == nil {
			return false
		}
		status := observed.Status.NodeMetricStatus
		return status.NodeMetric != nil && status.UpdateTime.After(oldTime.Time) && len(status.GroupMetric) == 0
	}, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, original.Labels, observed.Labels)
	require.Nil(t, observed.Status.NodeMetricStatus.GroupMetric)
	require.Equal(t, int64(200<<30), observed.Status.NodeMetricStatus.NodeMetric.GenericUsage.Memory().Value())
	require.Len(t, observed.Status.NodeMetricStatus.NodeMetric.NUMAUsage, metaServer.NumNUMANodes)
}

func TestNodeMetricUpdate(t *testing.T) {
	t.Parallel()

	type args struct {
		metricSetter func(f *metric.FakeMetricsFetcher, p *nodeMetricsReporterPlugin)
		pods         []*corev1.Pod
		containers   []types2.ContainerInfo
	}

	tests := []struct {
		name                 string
		args                 args
		wantErr              bool
		wantNodeMetricStatus *nodeapis.NodeMetricStatus
	}{
		{
			name: "no metrics: update failed",
			args: args{
				metricSetter: func(f *metric.FakeMetricsFetcher, p *nodeMetricsReporterPlugin) {
				},
			},
			wantErr: true,
		},
		{
			name: "update with dedicated_cores, shared_cores, reclaimed_cores pods",
			args: args{
				metricSetter: func(f *metric.FakeMetricsFetcher, p *nodeMetricsReporterPlugin) {
					f.SetNodeMetric(consts.MetricMemUsedSystem, utilmetric.MetricData{Value: 200 << 30})
					f.SetNodeMetric(consts.MetricMemPageCacheSystem, utilmetric.MetricData{Value: 40 << 30})
					f.SetNodeMetric(consts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5})
					for numaId := 0; numaId < 2; numaId++ {
						f.SetNumaMetric(numaId, consts.MetricMemUsedNuma, utilmetric.MetricData{Value: 50 << 30})
						f.SetNumaMetric(numaId, consts.MetricMemFilepageNuma, utilmetric.MetricData{Value: 10 << 30})
						f.SetNumaMetric(numaId, consts.MetricCPUUsageNuma, utilmetric.MetricData{Value: 2})
					}

					f.SetContainerMetric("uid1", "container1", consts.MetricCPUUsageContainer, utilmetric.MetricData{Value: 1})
					f.SetContainerMetric("uid1", "container1", consts.MetricMemUsageContainer, utilmetric.MetricData{Value: 20 << 30})
					f.SetContainerMetric("uid1", "container1", consts.MetricMemCacheContainer, utilmetric.MetricData{Value: 10 << 30})
					for numaId := 0; numaId < 2; numaId++ {
						f.SetContainerNumaMetric("uid1", "container1", numaId, consts.MetricsCPUUsageNUMAContainer, utilmetric.MetricData{Value: 1})
						f.SetContainerNumaMetric("uid1", "container1", numaId, consts.MetricsMemAnonPerNumaContainer, utilmetric.MetricData{Value: 1 << 30})
					}

					f.SetContainerMetric("uid2", "container2", consts.MetricCPUUsageContainer, utilmetric.MetricData{Value: 1})
					f.SetContainerMetric("uid2", "container2", consts.MetricMemUsageContainer, utilmetric.MetricData{Value: 20 << 30})
					f.SetContainerMetric("uid2", "container2", consts.MetricMemCacheContainer, utilmetric.MetricData{Value: 10 << 30})

					f.SetContainerMetric("uid3", "container3", consts.MetricCPUUsageContainer, utilmetric.MetricData{Value: 1})
					f.SetContainerMetric("uid3", "container3", consts.MetricMemUsageContainer, utilmetric.MetricData{Value: 20 << 30})
					f.SetContainerMetric("uid3", "container3", consts.MetricMemCacheContainer, utilmetric.MetricData{Value: 10 << 30})

					f.SetContainerMetric("uid4", "container4", consts.MetricCPUUsageContainer, utilmetric.MetricData{Value: 1})
					f.SetContainerMetric("uid4", "container4", consts.MetricMemUsageContainer, utilmetric.MetricData{Value: 20 << 30})
					f.SetContainerMetric("uid4", "container4", consts.MetricMemCacheContainer, utilmetric.MetricData{Value: 10 << 30})
				},
				pods: []*corev1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:        "pod1",
							Namespace:   "default",
							UID:         "uid1",
							Annotations: map[string]string{apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: "container1",
									Resources: corev1.ResourceRequirements{
										Limits: map[corev1.ResourceName]resource.Quantity{
											corev1.ResourceCPU:    resource.MustParse("4"),
											corev1.ResourceMemory: resource.MustParse("40Gi"),
										},
										Requests: map[corev1.ResourceName]resource.Quantity{
											corev1.ResourceCPU:    resource.MustParse("2"),
											corev1.ResourceMemory: resource.MustParse("20Gi"),
										},
									},
								},
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:        "pod2",
							Namespace:   "default",
							UID:         "uid2",
							Annotations: map[string]string{apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelDedicatedCores},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: "container2",
									Resources: corev1.ResourceRequirements{
										Limits: map[corev1.ResourceName]resource.Quantity{
											corev1.ResourceCPU:    resource.MustParse("4"),
											corev1.ResourceMemory: resource.MustParse("40Gi"),
										},
										Requests: map[corev1.ResourceName]resource.Quantity{
											corev1.ResourceCPU:    resource.MustParse("2"),
											corev1.ResourceMemory: resource.MustParse("20Gi"),
										},
									},
								},
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:        "pod3",
							Namespace:   "default",
							UID:         "uid3",
							Annotations: map[string]string{apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelReclaimedCores},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: "container3",
									Resources: corev1.ResourceRequirements{
										Limits: map[corev1.ResourceName]resource.Quantity{
											corev1.ResourceCPU:    resource.MustParse("4"),
											corev1.ResourceMemory: resource.MustParse("40Gi"),
										},
										Requests: map[corev1.ResourceName]resource.Quantity{
											corev1.ResourceCPU:    resource.MustParse("2"),
											corev1.ResourceMemory: resource.MustParse("20Gi"),
										},
									},
								},
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:        "pod4",
							Namespace:   "default",
							UID:         "uid4",
							Annotations: map[string]string{apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: "container4",
									Resources: corev1.ResourceRequirements{
										Limits: map[corev1.ResourceName]resource.Quantity{
											corev1.ResourceCPU:    resource.MustParse("4"),
											corev1.ResourceMemory: resource.MustParse("40Gi"),
										},
										Requests: map[corev1.ResourceName]resource.Quantity{
											corev1.ResourceCPU:    resource.MustParse("2"),
											corev1.ResourceMemory: resource.MustParse("20Gi"),
										},
									},
								},
							},
						},
					},
				},
				containers: []types2.ContainerInfo{
					{
						PodUID:         "uid1",
						PodNamespace:   "default",
						PodName:        "pod1",
						ContainerName:  "container1",
						ContainerType:  pluginapi.ContainerType_MAIN,
						ContainerIndex: 0,
					},
					{
						PodUID:         "uid2",
						PodNamespace:   "default",
						PodName:        "pod2",
						ContainerName:  "container2",
						ContainerType:  pluginapi.ContainerType_MAIN,
						ContainerIndex: 0,
					},
					{
						PodUID:         "uid3",
						PodNamespace:   "default",
						PodName:        "pod3",
						ContainerName:  "container3",
						ContainerType:  pluginapi.ContainerType_MAIN,
						ContainerIndex: 0,
					},
					{
						PodUID:                   "uid4",
						PodNamespace:             "default",
						PodName:                  "pod4",
						ContainerName:            "container4",
						ContainerType:            pluginapi.ContainerType_MAIN,
						ContainerIndex:           0,
						TopologyAwareAssignments: map[int]machine.CPUSet{0: machine.NewCPUSet(0), 1: machine.NewCPUSet(2)},
						RampUp:                   true,
					},
				},
			},
			wantErr: false,
			wantNodeMetricStatus: &nodeapis.NodeMetricStatus{
				UpdateTime: metav1.Time{},
				NodeMetric: &nodeapis.NodeMetricInfo{
					ResourceUsage: nodeapis.ResourceUsage{
						NUMAUsage: []nodeapis.NUMAMetricInfo{
							{
								NUMAId: 0,
								Usage: corev1.ResourceList{
									corev1.ResourceMemory: *mustParse("40Gi"),
									corev1.ResourceCPU:    *mustParse("2"),
								},
							},
							{
								NUMAId: 1,
								Usage: corev1.ResourceList{
									corev1.ResourceMemory: *mustParse("40Gi"),
									corev1.ResourceCPU:    *mustParse("2"),
								},
							},
						},
						GenericUsage: corev1.ResourceList{
							corev1.ResourceCPU:    *mustParse("8"),
							corev1.ResourceMemory: *mustParse("200Gi"),
						},
					},
				},
				GroupMetric: []nodeapis.GroupMetricInfo{
					{
						QoSLevel: apiconsts.PodAnnotationQoSLevelDedicatedCores,
						ResourceUsage: nodeapis.ResourceUsage{
							NUMAUsage: []nodeapis.NUMAMetricInfo{
								{
									NUMAId: 0,
									Usage: corev1.ResourceList{
										corev1.ResourceMemory: *mustParse("0"),
										corev1.ResourceCPU:    *mustParse("0"),
									},
								},
								{
									NUMAId: 1,
									Usage: corev1.ResourceList{
										corev1.ResourceMemory: *mustParse("0"),
										corev1.ResourceCPU:    *mustParse("0"),
									},
								},
							},
							GenericUsage: corev1.ResourceList{
								corev1.ResourceCPU:    *mustParse("1"),
								corev1.ResourceMemory: *mustParse("10Gi"),
							},
						},
						PodList: []string{"default/pod2"},
					},
					{
						QoSLevel: apiconsts.PodAnnotationQoSLevelSharedCores,
						ResourceUsage: nodeapis.ResourceUsage{
							NUMAUsage: []nodeapis.NUMAMetricInfo{
								{
									NUMAId: 0,
									Usage: corev1.ResourceList{
										corev1.ResourceCPU:    *mustParse("2"),
										corev1.ResourceMemory: *mustParse("11Gi"),
									},
								},
								{
									NUMAId: 1,
									Usage: corev1.ResourceList{
										corev1.ResourceCPU:    *mustParse("2"),
										corev1.ResourceMemory: *mustParse("11Gi"),
									},
								},
							},
							GenericUsage: corev1.ResourceList{
								corev1.ResourceCPU:    *mustParse("3"),
								corev1.ResourceMemory: *mustParse("30Gi"),
							},
						},
						PodList: []string{"default/pod1", "default/pod4"},
					},
					{
						QoSLevel: apiconsts.PodAnnotationQoSLevelReclaimedCores,
						ResourceUsage: nodeapis.ResourceUsage{
							NUMAUsage: []nodeapis.NUMAMetricInfo{
								{
									NUMAId: 0,
									Usage: corev1.ResourceList{
										corev1.ResourceMemory: *mustParse("0"),
										corev1.ResourceCPU:    *mustParse("0"),
									},
								},
								{
									NUMAId: 1,
									Usage: corev1.ResourceList{
										corev1.ResourceMemory: *mustParse("0"),
										corev1.ResourceCPU:    *mustParse("0"),
									},
								},
							},
							GenericUsage: corev1.ResourceList{
								corev1.ResourceCPU:    *mustParse("1"),
								corev1.ResourceMemory: *mustParse("10Gi"),
							},
						},
						PodList: []string{"default/pod3"},
					},
					{
						QoSLevel: apiconsts.PodAnnotationQoSLevelSystemCores,
						ResourceUsage: nodeapis.ResourceUsage{
							NUMAUsage: []nodeapis.NUMAMetricInfo{
								{
									NUMAId: 0,
									Usage: corev1.ResourceList{
										corev1.ResourceMemory: *mustParse("0"),
										corev1.ResourceCPU:    *mustParse("0"),
									},
								},
								{
									NUMAId: 1,
									Usage: corev1.ResourceList{
										corev1.ResourceMemory: *mustParse("0"),
										corev1.ResourceCPU:    *mustParse("0"),
									},
								},
							},
							GenericUsage: corev1.ResourceList{
								corev1.ResourceCPU:    *mustParse("0"),
								corev1.ResourceMemory: *mustParse("0"),
							},
						},
						PodList: nil,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			regDir, ckDir, statDir, err := tmpDirs()
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = os.RemoveAll(regDir)
				_ = os.RemoveAll(ckDir)
				_ = os.RemoveAll(statDir)
			})

			conf := generateTestConfiguration(t, regDir, ckDir, statDir)
			clientSet := generateTestGenericClientSet(nil, nil)
			metaServer := generateTestMetaServer(clientSet, conf, tt.args.pods...)
			metaReader := generateTestMetaCache(t, conf, metaServer.MetricsFetcher, tt.args.containers...)

			reporter, err := NewNodeMetricsReporter(metrics.DummyMetrics{}, metaServer, metaReader, conf)

			require.NoError(t, err)
			require.NotNil(t, reporter)

			fakeMetricsFetcher := metaServer.MetricsFetcher.(*metric.FakeMetricsFetcher)
			require.NotNil(t, fakeMetricsFetcher)
			reporterImpl, ok := reporter.(*nodeMetricsReporterImpl)
			require.True(t, ok)
			pluginWrapper, ok := reporterImpl.GenericPlugin.(*skeleton.PluginRegistrationWrapper)
			require.True(t, ok)
			plugin, ok := pluginWrapper.GenericPlugin.(*nodeMetricsReporterPlugin)
			require.True(t, ok)
			tt.args.metricSetter(fakeMetricsFetcher, plugin)

			ctx, cancel := context.WithCancel(context.Background())
			wg := sync.WaitGroup{}
			wg.Add(1)

			go func() {
				defer wg.Done()
				reporter.Run(ctx)
			}()

			var (
				resp   *v1alpha1.GetReportContentResponse
				gotErr error
			)
			require.Eventually(t, func() bool {
				resp, gotErr = plugin.GetReportContent(context.TODO(), nil)
				if tt.wantErr {
					return gotErr != nil
				}
				return gotErr == nil && resp != nil && len(resp.Content) > 0
			}, 5*time.Second, 100*time.Millisecond)

			if gotErr != nil {
				require.True(t, tt.wantErr)
			} else {
				require.False(t, tt.wantErr)
				require.Equal(t, &util.CNRGroupVersionKind, resp.Content[0].GroupVersionKind)
				require.Equal(t, v1alpha1.FieldType_Status, resp.Content[0].Field[0].FieldType)
				require.Equal(t, util.CNRFieldNameNodeMetricStatus, resp.Content[0].Field[0].FieldName)

				nodeMetricStatus := nodeapis.NodeMetricStatus{}
				err = json.Unmarshal(resp.Content[0].Field[0].Value, &nodeMetricStatus)
				require.NoError(t, err)
				tt.wantNodeMetricStatus.UpdateTime = *nodeMetricStatus.UpdateTime.DeepCopy()
				// to sort numa usage slice  to avoid flaky equality check
				for _, groupMetric := range nodeMetricStatus.GroupMetric {
					slice := groupMetric.ResourceUsage.NUMAUsage
					sort.Slice(slice, func(i, j int) bool {
						return slice[i].NUMAId < slice[j].NUMAId
					})
				}
				require.Equal(t, *tt.wantNodeMetricStatus, nodeMetricStatus)
			}

			cancel()
			wg.Wait()
		})
	}
}
