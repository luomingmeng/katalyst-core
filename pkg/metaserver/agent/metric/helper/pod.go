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

package helper

import (
	"errors"
	"strconv"

	v1 "k8s.io/api/core/v1"

	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
)

// GetPodMetric returns the value of a pod-level metric.
// Ordinary Pods are aggregated from containers; whole-workload projections use
// MetricsReader.GetPodMetric without falling back on unavailable metrics.
func GetPodMetric(metricsFetcher types.MetricsFetcher, emitter metrics.MetricEmitter, pod *v1.Pod, metricName string, numaID int) (float64, error) {
	if pod == nil {
		return 0, errors.New("nil pod")
	}

	var podMetricValue float64
	data, handled, err := metricsFetcher.GetPodMetric(pod, metricName, numaID)
	if handled {
		if err != nil {
			return 0, err
		}
		podMetricValue = data.Value
	}
	if !handled {
		for _, container := range pod.Spec.Containers {
			containerMetricValue, err := GetContainerMetric(metricsFetcher, emitter, string(pod.UID), container.Name, metricName, numaID)
			if err != nil {
				return 0, err
			}

			podMetricValue += containerMetricValue
		}
	}

	_ = emitter.StoreFloat64(metricsNamePodMetric, podMetricValue, metrics.MetricTypeNameRaw,
		metrics.ConvertMapToTags(map[string]string{
			metricsTagKeyPodUID:     string(pod.UID),
			metricsTagKeyNumaID:     strconv.Itoa(numaID),
			metricsTagKeyMetricName: metricName,
		})...)

	return podMetricValue, nil
}
