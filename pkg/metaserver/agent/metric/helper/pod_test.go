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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

type podMetricReader struct {
	*metric.FakeMetricsFetcher
	handled bool
	err     error
}

func (f *podMetricReader) GetPodMetric(_ *v1.Pod, _ string, _ int) (utilmetric.MetricData, bool, error) {
	return utilmetric.MetricData{Value: 7}, f.handled, f.err
}

func TestGetPodMetricOrdinaryAggregationAndUnavailableTarget(t *testing.T) {
	t.Parallel()
	fetcher := &podMetricReader{FakeMetricsFetcher: metric.NewFakeMetricsFetcher(metrics.DummyMetrics{}).(*metric.FakeMetricsFetcher)}
	fetcher.RegisterExternalMetric(func(store *utilmetric.MetricStore) {
		store.SetContainerMetric("target", "one", consts.MetricMemRssContainer, utilmetric.MetricData{Value: 2})
		store.SetContainerMetric("target", "two", consts.MetricMemRssContainer, utilmetric.MetricData{Value: 3})
	})
	fetcher.Run(context.Background())
	p := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "target"}, Spec: v1.PodSpec{Containers: []v1.Container{{Name: "one"}, {Name: "two"}}}}
	value, err := GetPodMetric(fetcher, metrics.DummyMetrics{}, p, consts.MetricMemRssContainer, -1)
	assert.NoError(t, err)
	assert.Equal(t, float64(5), value)
	fetcher.handled = true
	fetcher.err = errors.New("sample unavailable")
	value, err = GetPodMetric(fetcher, metrics.DummyMetrics{}, p, consts.MetricMemRssContainer, -1)
	assert.ErrorIs(t, err, fetcher.err)
	assert.Zero(t, value)
}

func TestGetPodMetricOrdinaryFailuresRemainStrict(t *testing.T) {
	t.Parallel()
	for _, numaID := range []int{-1, 0} {
		for _, expired := range []bool{false, true} {
			fetcher := metric.NewFakeMetricsFetcher(metrics.DummyMetrics{}).(*metric.FakeMetricsFetcher)
			p := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "ordinary"}, Spec: v1.PodSpec{Containers: []v1.Container{{Name: "one"}, {Name: "two"}}}}
			metricName := consts.MetricMemRssContainer
			if numaID >= 0 {
				metricName = consts.MetricsMemTotalPerNumaContainer
			}
			now := time.Now()
			for _, container := range p.Spec.Containers {
				if container.Name == "two" && !expired {
					continue
				}
				data := utilmetric.MetricData{Value: 3, Time: &now}
				if container.Name == "two" {
					old := now.Add(-2 * time.Minute)
					data.Time = &old
				}
				if numaID >= 0 {
					fetcher.SetContainerNumaMetric(string(p.UID), container.Name, numaID, metricName, data)
				} else {
					fetcher.SetContainerMetric(string(p.UID), container.Name, metricName, data)
				}
			}
			value, err := GetPodMetric(fetcher, metrics.DummyMetrics{}, p, metricName, numaID)
			assert.Error(t, err)
			assert.Zero(t, value)
		}
	}
}

func TestGetPodMetricUsesPodValueWithoutContainerAggregation(t *testing.T) {
	t.Parallel()
	fetcher := &podMetricReader{FakeMetricsFetcher: metric.NewFakeMetricsFetcher(metrics.DummyMetrics{}).(*metric.FakeMetricsFetcher), handled: true}
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "target"}, Spec: v1.PodSpec{Containers: []v1.Container{{Name: "one"}, {Name: "two"}}}}
	value, err := GetPodMetric(fetcher, metrics.DummyMetrics{}, pod, consts.MetricMemRssContainer, -1)
	assert.NoError(t, err)
	assert.Equal(t, float64(7), value)
}
