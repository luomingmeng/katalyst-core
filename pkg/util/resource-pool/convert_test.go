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

package resourcepool

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nodev1alpha1 "github.com/kubewharf/katalyst-api/pkg/apis/node/v1alpha1"
)

func TestConvertResourcePoolsRoundTrip(t *testing.T) {
	t.Parallel()

	var (
		max nodev1alpha1.Aggregator = "max"
		min nodev1alpha1.Aggregator = "min"
	)

	cases := []struct {
		name          string
		resourcePools []NumaResourcePool
		want          []nodev1alpha1.ScopedNodeMetrics
	}{
		{
			name: "normal",
			resourcePools: []NumaResourcePool{
				{
					NumaID: NumaIDAll,
					ResourcePools: []nodev1alpha1.ResourcePool{
						{
							PoolName: "L1",
							MinAllocatable: &v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1"),
								v1.ResourceMemory: resource.MustParse("1Gi"),
							},
							MaxAllocatable: &v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("4"),
								v1.ResourceMemory: resource.MustParse("4Gi"),
							},
						},
						{
							PoolName: "L2",
							MaxAllocatable: &v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("8"),
								v1.ResourceMemory: resource.MustParse("8Gi"),
							},
						},
					},
				},
			},
			want: []nodev1alpha1.ScopedNodeMetrics{
				{
					Scope: "resource-pool",
					Metrics: []nodev1alpha1.MetricValue{
						{
							MetricName: "cpu",
							MetricLabels: map[string]string{
								"pool-name": "L1",
							},
							Aggregator: &max,
							Value:      resource.MustParse("4"),
						},
						{
							MetricName: "memory",
							MetricLabels: map[string]string{
								"pool-name": "L1",
							},
							Aggregator: &max,
							Value:      resource.MustParse("4Gi"),
						},
						{
							MetricName: "cpu",
							MetricLabels: map[string]string{
								"pool-name": "L1",
							},
							Aggregator: &min,
							Value:      resource.MustParse("1"),
						},
						{
							MetricName: "memory",
							MetricLabels: map[string]string{
								"pool-name": "L1",
							},
							Aggregator: &min,
							Value:      resource.MustParse("1Gi"),
						},
						{
							MetricName: "cpu",
							MetricLabels: map[string]string{
								"pool-name": "L2",
							},
							Aggregator: &max,
							Value:      resource.MustParse("8"),
						},
						{
							MetricName: "memory",
							MetricLabels: map[string]string{
								"pool-name": "L2",
							},
							Aggregator: &max,
							Value:      resource.MustParse("8Gi"),
						},
					},
				},
			},
		},
		{
			name:          "nil",
			resourcePools: nil,
			want: []nodev1alpha1.ScopedNodeMetrics{
				{
					Scope: "resource-pool",
				},
			},
		},
		{
			name: "numa",
			resourcePools: []NumaResourcePool{
				{
					NumaID: NumaIDAll,
					ResourcePools: []nodev1alpha1.ResourcePool{
						{
							PoolName: "L1",
							MaxAllocatable: &v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("4"),
							},
							Attributes: []nodev1alpha1.Attribute{
								{Name: "partition", Value: "flink"},
							},
						},
					},
				}, {
					NumaID: 0,
					ResourcePools: []nodev1alpha1.ResourcePool{
						{
							PoolName: "L1",
							MaxAllocatable: &v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1"),
								v1.ResourceMemory: resource.MustParse("1Gi"),
							},
							Attributes: []nodev1alpha1.Attribute{
								{Name: "partition", Value: "flink"},
							},
						},
					},
				}, {
					NumaID: 1,
					ResourcePools: []nodev1alpha1.ResourcePool{
						{
							PoolName: "L1",
							MaxAllocatable: &v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("2"),
								v1.ResourceMemory: resource.MustParse("2Gi"),
							},
							Attributes: []nodev1alpha1.Attribute{
								{Name: "partition", Value: "flink"},
							},
						},
					},
				},
			},
			want: []nodev1alpha1.ScopedNodeMetrics{
				{
					Scope: "resource-pool",
					Metrics: []nodev1alpha1.MetricValue{
						{
							MetricName: "cpu",
							MetricLabels: map[string]string{
								"pool-name": "L1",
								"partition": "flink",
							},
							Aggregator: &max,
							Value:      resource.MustParse("4"),
						}, {
							MetricName: "cpu",
							MetricLabels: map[string]string{
								"pool-name": "L1",
								"numa-id":   "0",
								"partition": "flink",
							},
							Aggregator: &max,
							Value:      resource.MustParse("1"),
						}, {
							MetricName: "memory",
							MetricLabels: map[string]string{
								"pool-name": "L1",
								"numa-id":   "0",
								"partition": "flink",
							},
							Aggregator: &max,
							Value:      resource.MustParse("1Gi"),
						}, {
							MetricName: "cpu",
							MetricLabels: map[string]string{
								"pool-name": "L1",
								"numa-id":   "1",
								"partition": "flink",
							},
							Aggregator: &max,
							Value:      resource.MustParse("2"),
						}, {
							MetricName: "memory",
							MetricLabels: map[string]string{
								"pool-name": "L1",
								"numa-id":   "1",
								"partition": "flink",
							},
							Aggregator: &max,
							Value:      resource.MustParse("2Gi"),
						},
					},
				},
			},
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := ConvertResourcePoolsToNPDMetrics(tt.resourcePools, metav1.Time{})
			if !apiequality.Semantic.DeepEqual(got, tt.want) {
				t.Errorf("ConvertResourcePoolsToNPDMetrics()\ngot  %v\nwant %v", got, tt.want)
			}
			// convert back
			pools := ConvertNPDMetricsToResourcePools(got)
			if !apiequality.Semantic.DeepEqual(pools, tt.resourcePools) {
				t.Errorf("ConvertNPDMetricsToResourcePools() got \n%v, want \n%v", pools, tt.resourcePools)
			}
		})
	}
}
