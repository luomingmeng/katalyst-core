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

package strategy

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
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"
	maputil "k8s.io/kubernetes/pkg/util/maps"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	evictionpluginapi "github.com/kubewharf/katalyst-api/pkg/protocol/evictionplugin/v1alpha1"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	qrmstate "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/config"
	pkgconsts "github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

const (
	defaultCPUMaxSuppressionToleranceRate     = 5.0
	defaultCPUMinSuppressionToleranceDuration = 10 * time.Millisecond
)

func makeSuppressionEvictionConf(cpuMaxSuppressionToleranceRate float64,
	cpuMinSuppressionToleranceDuration time.Duration,
) *config.Configuration {
	conf := config.NewConfiguration()
	conf.GetDynamicConfiguration().EnableSuppressionEviction = true
	conf.GetDynamicConfiguration().MaxSuppressionToleranceRate = cpuMaxSuppressionToleranceRate
	conf.GetDynamicConfiguration().MinSuppressionToleranceDuration = cpuMinSuppressionToleranceDuration
	conf.ReclaimRelativeRootCgroupPath = "test"
	return conf
}

func TestNewCPUPressureSuppressionEviction(t *testing.T) {
	t.Parallel()

	as := require.New(t)

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	as.Nil(err)
	conf := makeSuppressionEvictionConf(defaultCPUMaxSuppressionToleranceRate, defaultCPUMinSuppressionToleranceDuration)
	metaServer := makeMetaServer(metric.NewFakeMetricsFetcher(metrics.DummyMetrics{}), cpuTopology)
	stateImpl, err := makeState(cpuTopology)
	as.Nil(err)

	plugin, _ := NewCPUPressureSuppressionEviction(metrics.DummyMetrics{}, metaServer, conf, stateImpl)
	as.NotNil(plugin)
}

func TestCPUPressureSuppression_GetEvictPods(t *testing.T) {
	t.Parallel()

	as := require.New(t)

	now := time.Now()

	pod1UID := string(uuid.NewUUID())
	pod1Name := "pod-1"
	pod2UID := string(uuid.NewUUID())
	pod2Name := "pod-2"

	tests := []struct {
		name               string
		podEntries         qrmstate.PodEntries
		setFakeMetric      func(store *metric.FakeMetricsFetcher)
		wantEvictPodUIDSet sets.String
	}{
		{
			name: "no over tolerance rate pod",
			podEntries: qrmstate.PodEntries{
				pod1UID: qrmstate.ContainerEntries{
					pod1Name: &qrmstate.AllocationInfo{
						AllocationMeta: commonstate.AllocationMeta{
							PodUid:         pod1UID,
							PodNamespace:   pod1Name,
							PodName:        pod1Name,
							ContainerName:  pod1Name,
							ContainerType:  pluginapi.ContainerType_MAIN.String(),
							ContainerIndex: 0,
							OwnerPoolName:  commonstate.PoolNameReclaim,
							Labels: map[string]string{
								apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelReclaimedCores,
							},
							Annotations: map[string]string{
								apiconsts.PodAnnotationQoSLevelKey:       apiconsts.PodAnnotationQoSLevelReclaimedCores,
								apiconsts.PodAnnotationCPUEnhancementKey: `{"suppression_tolerance_rate": "1.2"}`,
							},
							QoSLevel: apiconsts.PodAnnotationQoSLevelReclaimedCores,
						},
						RampUp:                   false,
						AllocationResult:         machine.MustParse("1,3-6,9,11-14"),
						OriginalAllocationResult: machine.MustParse("1,3-6,9,11-14"),
						TopologyAwareAssignments: map[int]machine.CPUSet{
							0: machine.NewCPUSet(1, 9),
							1: machine.NewCPUSet(3, 11),
							2: machine.NewCPUSet(4, 5, 11, 12),
							3: machine.NewCPUSet(6, 14),
						},
						OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
							0: machine.NewCPUSet(1, 9),
							1: machine.NewCPUSet(3, 11),
							2: machine.NewCPUSet(4, 5, 11, 12),
							3: machine.NewCPUSet(6, 14),
						},
						RequestQuantity: 2,
					},
				},
				commonstate.PoolNameReclaim: qrmstate.ContainerEntries{
					"": &qrmstate.AllocationInfo{
						AllocationMeta:           commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
						AllocationResult:         machine.MustParse("1,3-6,9,11-14"),
						OriginalAllocationResult: machine.MustParse("1,3-6,9,11-14"),
						TopologyAwareAssignments: map[int]machine.CPUSet{
							0: machine.NewCPUSet(1, 9),
							1: machine.NewCPUSet(3, 11),
							2: machine.NewCPUSet(4, 5, 11, 12),
							3: machine.NewCPUSet(6, 14),
						},
						OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
							0: machine.NewCPUSet(1, 9),
							1: machine.NewCPUSet(3, 11),
							2: machine.NewCPUSet(4, 5, 11, 12),
							3: machine.NewCPUSet(6, 14),
						},
					},
				},
			},
			wantEvictPodUIDSet: sets.NewString(),
			setFakeMetric: func(store *metric.FakeMetricsFetcher) {
				store.SetCPUMetric(1, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(3, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(4, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(5, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(6, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(9, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(11, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(12, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(13, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(14, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})

				store.SetCgroupMetric("test", pkgconsts.MetricCPUUsageCgroup, utilmetric.MetricData{Value: 5, Time: &now})
				store.SetCgroupMetric("test", pkgconsts.MetricCPUQuotaCgroup, utilmetric.MetricData{Value: 20000, Time: &now})
				store.SetCgroupMetric("test", pkgconsts.MetricCPUPeriodCgroup, utilmetric.MetricData{Value: 1000, Time: &now})
			},
		},
		{
			name: "over tolerance rate",
			podEntries: qrmstate.PodEntries{
				pod1UID: qrmstate.ContainerEntries{
					pod1Name: &qrmstate.AllocationInfo{
						AllocationMeta: commonstate.AllocationMeta{
							PodUid:         pod1UID,
							PodNamespace:   pod1Name,
							PodName:        pod1Name,
							ContainerName:  pod1Name,
							ContainerType:  pluginapi.ContainerType_MAIN.String(),
							ContainerIndex: 0,
							OwnerPoolName:  commonstate.PoolNameReclaim,
							Labels: map[string]string{
								apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelReclaimedCores,
							},
							Annotations: map[string]string{
								apiconsts.PodAnnotationQoSLevelKey:       apiconsts.PodAnnotationQoSLevelReclaimedCores,
								apiconsts.PodAnnotationCPUEnhancementKey: `{"suppression_tolerance_rate": "1.2"}`,
							},
							QoSLevel: apiconsts.PodAnnotationQoSLevelReclaimedCores,
						},
						RampUp:                   false,
						AllocationResult:         machine.MustParse("1,3-6,9,11-14"),
						OriginalAllocationResult: machine.MustParse("1,3-6,9,11-14"),
						TopologyAwareAssignments: map[int]machine.CPUSet{
							0: machine.NewCPUSet(1, 9),
							1: machine.NewCPUSet(3, 11),
							2: machine.NewCPUSet(4, 5, 11, 12),
							3: machine.NewCPUSet(6, 14),
						},
						OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
							0: machine.NewCPUSet(1, 9),
							1: machine.NewCPUSet(3, 11),
							2: machine.NewCPUSet(4, 5, 11, 12),
							3: machine.NewCPUSet(6, 14),
						},
						RequestQuantity: 15,
					},
				},
				pod2UID: qrmstate.ContainerEntries{
					pod1Name: &qrmstate.AllocationInfo{
						AllocationMeta: commonstate.AllocationMeta{
							PodUid:         pod2UID,
							PodNamespace:   pod2Name,
							PodName:        pod2Name,
							ContainerName:  pod2Name,
							ContainerType:  pluginapi.ContainerType_MAIN.String(),
							ContainerIndex: 0,
							OwnerPoolName:  commonstate.PoolNameReclaim,
							Labels: map[string]string{
								apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelReclaimedCores,
							},
							Annotations: map[string]string{
								apiconsts.PodAnnotationQoSLevelKey:       apiconsts.PodAnnotationQoSLevelReclaimedCores,
								apiconsts.PodAnnotationCPUEnhancementKey: `{"suppression_tolerance_rate": "1.2"}`,
							},
							QoSLevel: apiconsts.PodAnnotationQoSLevelReclaimedCores,
						},
						RampUp:                   false,
						AllocationResult:         machine.MustParse("1,3-6,9,11-14"),
						OriginalAllocationResult: machine.MustParse("1,3-6,9,11-14"),
						TopologyAwareAssignments: map[int]machine.CPUSet{
							0: machine.NewCPUSet(1, 9),
							1: machine.NewCPUSet(3, 11),
							2: machine.NewCPUSet(4, 5, 11, 12),
							3: machine.NewCPUSet(6, 14),
						},
						OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
							0: machine.NewCPUSet(1, 9),
							1: machine.NewCPUSet(3, 11),
							2: machine.NewCPUSet(4, 5, 11, 12),
							3: machine.NewCPUSet(6, 14),
						},
						RequestQuantity: 4,
					},
				},
				commonstate.PoolNameReclaim: qrmstate.ContainerEntries{
					"": &qrmstate.AllocationInfo{
						AllocationMeta:           commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
						AllocationResult:         machine.MustParse("1,3-6,9,11-14"),
						OriginalAllocationResult: machine.MustParse("1,3-6,9,11-14"),
						TopologyAwareAssignments: map[int]machine.CPUSet{
							0: machine.NewCPUSet(1, 9),
							1: machine.NewCPUSet(3, 11),
							2: machine.NewCPUSet(4, 5, 11, 12),
							3: machine.NewCPUSet(6, 14),
						},
						OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
							0: machine.NewCPUSet(1, 9),
							1: machine.NewCPUSet(3, 11),
							2: machine.NewCPUSet(4, 5, 11, 12),
							3: machine.NewCPUSet(6, 14),
						},
					},
				},
			},
			wantEvictPodUIDSet: sets.NewString(pod1UID),
			setFakeMetric: func(store *metric.FakeMetricsFetcher) {
				store.SetCPUMetric(1, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(3, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(4, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(5, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(6, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(9, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(11, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(12, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(13, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(14, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})

				store.SetCgroupMetric("test", pkgconsts.MetricCPUUsageCgroup, utilmetric.MetricData{Value: 5, Time: &now})
				store.SetCgroupMetric("test", pkgconsts.MetricCPUQuotaCgroup, utilmetric.MetricData{Value: 20000, Time: &now})
				store.SetCgroupMetric("test", pkgconsts.MetricCPUPeriodCgroup, utilmetric.MetricData{Value: 1000, Time: &now})
			},
		},
		{
			name: "over tolerance rate, because quota limited",
			podEntries: qrmstate.PodEntries{
				pod1UID: qrmstate.ContainerEntries{
					pod1Name: &qrmstate.AllocationInfo{
						AllocationMeta: commonstate.AllocationMeta{
							PodUid:         pod1UID,
							PodNamespace:   pod1Name,
							PodName:        pod1Name,
							ContainerName:  pod1Name,
							ContainerType:  pluginapi.ContainerType_MAIN.String(),
							ContainerIndex: 0,
							OwnerPoolName:  commonstate.PoolNameReclaim,
							Labels: map[string]string{
								apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelReclaimedCores,
							},
							Annotations: map[string]string{
								apiconsts.PodAnnotationQoSLevelKey:       apiconsts.PodAnnotationQoSLevelReclaimedCores,
								apiconsts.PodAnnotationCPUEnhancementKey: `{"suppression_tolerance_rate": "1.2"}`,
							},
							QoSLevel: apiconsts.PodAnnotationQoSLevelReclaimedCores,
						},
						RampUp:                   false,
						AllocationResult:         machine.MustParse("1,3-6,9,11-14"),
						OriginalAllocationResult: machine.MustParse("1,3-6,9,11-14"),
						TopologyAwareAssignments: map[int]machine.CPUSet{
							0: machine.NewCPUSet(1, 9),
							1: machine.NewCPUSet(3, 11),
							2: machine.NewCPUSet(4, 5, 11, 12),
							3: machine.NewCPUSet(6, 14),
						},
						OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
							0: machine.NewCPUSet(1, 9),
							1: machine.NewCPUSet(3, 11),
							2: machine.NewCPUSet(4, 5, 11, 12),
							3: machine.NewCPUSet(6, 14),
						},
						RequestQuantity: 15,
					},
				},
				pod2UID: qrmstate.ContainerEntries{
					pod1Name: &qrmstate.AllocationInfo{
						AllocationMeta: commonstate.AllocationMeta{
							PodUid:         pod2UID,
							PodNamespace:   pod2Name,
							PodName:        pod2Name,
							ContainerName:  pod2Name,
							ContainerType:  pluginapi.ContainerType_MAIN.String(),
							ContainerIndex: 0,
							OwnerPoolName:  commonstate.PoolNameReclaim,
							Labels: map[string]string{
								apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelReclaimedCores,
							},
							Annotations: map[string]string{
								apiconsts.PodAnnotationQoSLevelKey:       apiconsts.PodAnnotationQoSLevelReclaimedCores,
								apiconsts.PodAnnotationCPUEnhancementKey: `{"suppression_tolerance_rate": "1.2"}`,
							},
							QoSLevel: apiconsts.PodAnnotationQoSLevelReclaimedCores,
						},
						RampUp:                   false,
						AllocationResult:         machine.MustParse("1,3-6,9,11-14"),
						OriginalAllocationResult: machine.MustParse("1,3-6,9,11-14"),
						TopologyAwareAssignments: map[int]machine.CPUSet{
							0: machine.NewCPUSet(1, 9),
							1: machine.NewCPUSet(3, 11),
							2: machine.NewCPUSet(4, 5, 11, 12),
							3: machine.NewCPUSet(6, 14),
						},
						OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
							0: machine.NewCPUSet(1, 9),
							1: machine.NewCPUSet(3, 11),
							2: machine.NewCPUSet(4, 5, 11, 12),
							3: machine.NewCPUSet(6, 14),
						},
						RequestQuantity: 4,
					},
				},
				commonstate.PoolNameReclaim: qrmstate.ContainerEntries{
					"": &qrmstate.AllocationInfo{
						AllocationMeta:           commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
						AllocationResult:         machine.MustParse("1,3-6,9,11-14"),
						OriginalAllocationResult: machine.MustParse("1,3-6,9,11-14"),
						TopologyAwareAssignments: map[int]machine.CPUSet{
							0: machine.NewCPUSet(1, 9),
							1: machine.NewCPUSet(3, 11),
							2: machine.NewCPUSet(4, 5, 11, 12),
							3: machine.NewCPUSet(6, 14),
						},
						OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
							0: machine.NewCPUSet(1, 9),
							1: machine.NewCPUSet(3, 11),
							2: machine.NewCPUSet(4, 5, 11, 12),
							3: machine.NewCPUSet(6, 14),
						},
					},
				},
			},
			wantEvictPodUIDSet: sets.NewString(pod1UID),
			setFakeMetric: func(store *metric.FakeMetricsFetcher) {
				store.SetCPUMetric(1, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(3, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(4, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(5, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(6, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(9, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(11, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(12, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(13, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})
				store.SetCPUMetric(14, pkgconsts.MetricCPUUsageRatio, utilmetric.MetricData{Value: 0.5, Time: &now})

				store.SetCgroupMetric("test", pkgconsts.MetricCPUUsageCgroup, utilmetric.MetricData{Value: 55, Time: &now})
				store.SetCgroupMetric("test", pkgconsts.MetricCPUQuotaCgroup, utilmetric.MetricData{Value: 5000, Time: &now})
				store.SetCgroupMetric("test", pkgconsts.MetricCPUPeriodCgroup, utilmetric.MetricData{Value: 1000, Time: &now})
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
			as.Nil(err)
			conf := makeSuppressionEvictionConf(defaultCPUMaxSuppressionToleranceRate, defaultCPUMinSuppressionToleranceDuration)

			metricsFetcher := metric.NewFakeMetricsFetcher(metrics.DummyMetrics{})
			store := metricsFetcher.(*metric.FakeMetricsFetcher)

			metaServer := makeMetaServer(metricsFetcher, cpuTopology)
			stateImpl, err := makeState(cpuTopology)
			as.Nil(err)

			plugin, _ := NewCPUPressureSuppressionEviction(metrics.DummyMetrics{}, metaServer, conf, stateImpl)
			as.NotNil(plugin)

			pods := make([]*v1.Pod, 0, len(tt.podEntries))

			if tt.setFakeMetric != nil {
				tt.setFakeMetric(store)
			}

			for entryName, entries := range tt.podEntries {
				for subEntryName, entry := range entries {
					stateImpl.SetAllocationInfo(entryName, subEntryName, entry, true)

					if entries.IsPoolEntry() {
						continue
					}

					pod := &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							UID:         types.UID(entry.PodUid),
							Name:        entry.PodName,
							Namespace:   entry.PodNamespace,
							Annotations: maputil.CopySS(entry.Annotations),
							Labels:      maputil.CopySS(entry.Labels),
						},
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name: entry.ContainerName,
									Resources: v1.ResourceRequirements{
										Requests: v1.ResourceList{
											apiconsts.ReclaimedResourceMilliCPU: *resource.NewQuantity(int64(entry.RequestQuantity*1000), resource.DecimalSI),
										},
										Limits: v1.ResourceList{
											apiconsts.ReclaimedResourceMilliCPU: *resource.NewQuantity(int64(entry.RequestQuantity*1000), resource.DecimalSI),
										},
									},
								},
							},
						},
					}

					pods = append(pods, pod)
				}
			}

			plugin.(*CPUPressureSuppression).state = stateImpl

			resp, err := plugin.GetEvictPods(context.TODO(), &evictionpluginapi.GetEvictPodsRequest{
				ActivePods: pods,
			})
			assert.NoError(t, err)
			assert.NotNil(t, resp)

			time.Sleep(defaultCPUMinSuppressionToleranceDuration)

			resp, err = plugin.GetEvictPods(context.TODO(), &evictionpluginapi.GetEvictPodsRequest{
				ActivePods: pods,
			})
			assert.NoError(t, err)
			assert.NotNil(t, resp)

			evictPodUIDSet := sets.String{}
			for _, pod := range resp.EvictPods {
				evictPodUIDSet.Insert(string(pod.Pod.GetUID()))
			}
			assert.Equal(t, tt.wantEvictPodUIDSet, evictPodUIDSet)
		})
	}
}
