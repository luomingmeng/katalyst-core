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

package metric

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPodMetricStoreMappingFailureAndGC(t *testing.T) {
	t.Parallel()
	store := NewMetricStore()
	sample := PodMetricData{CgroupPath: "/target", MappingRevision: 2, Metrics: map[string]MetricData{"usage": {Value: 7}}, NumaMetrics: map[int]map[string]MetricData{0: {"usage": {Value: 3}}}}
	store.SetPodMetrics("target", sample)
	store.SetContainerMetric("ordinary", "one", "usage", MetricData{Value: 5})
	store.SetCgroupMetric("/shared", "usage", MetricData{Value: 6})
	store.SetPodMetrics("target", PodMetricData{CgroupPath: "/old", MappingRevision: 1})
	data, err := store.GetPodMetric("target", "/target", 2, "usage", -1)
	require.NoError(t, err)
	assert.Equal(t, float64(7), data.Value)
	_, err = store.GetPodMetric("target", "/target", 1, "usage", -1)
	require.Error(t, err)
	_, err = store.GetPodMetric("target", "/target", 2, "numa.usage", 0)
	require.Error(t, err)
	_, err = store.GetPodMetric("target", "/target", 2, "usage", -1)
	require.NoError(t, err)
	data, err = store.GetPodMetric("target", "/target", 2, "usage", 0)
	require.NoError(t, err)
	assert.Equal(t, float64(3), data.Value)
	store.SetPodMetrics("target", PodMetricData{CgroupPath: "/target", MappingRevision: 2})
	_, err = store.GetPodMetric("target", "/target", 2, "usage", -1)
	require.Error(t, err)
	_, err = store.GetPodMetric("target", "/target", 2, "usage", 0)
	require.Error(t, err)
	store.GCPodMetricSamples(map[string]bool{"target": true})
	assert.Len(t, store.podMetricMap, 1)
	store.GCPodMetricSamples(nil)
	assert.Empty(t, store.podMetricMap)
	_, err = store.GetContainerMetric("ordinary", "one", "usage")
	require.NoError(t, err)
	_, err = store.GetCgroupMetric("/shared", "usage")
	require.NoError(t, err)
}

func TestPodMetricStoreConcurrentPublishRead(t *testing.T) {
	t.Parallel()
	store := NewMetricStore()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				store.SetPodMetrics("target", PodMetricData{CgroupPath: "/target", MappingRevision: 1, Metrics: map[string]MetricData{"usage": {Value: 7}}})
				data, err := store.GetPodMetric("target", "/target", 1, "usage", -1)
				if assert.NoError(t, err) {
					assert.Equal(t, float64(7), data.Value)
				}
			}
		}()
	}
	wg.Wait()
}
