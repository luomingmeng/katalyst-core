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
	"errors"
	"fmt"
)

var ErrPodMetricUnsupported = errors.New("whole-workload metric unsupported")

// PodMetricData contains only valid metrics from one whole-workload sample.
// Path and revision bind it to the inventory mapping sampled. Failed resources
// are absent; a failed shared collection publishes an empty sample.
type PodMetricData struct {
	CgroupPath      string
	MappingRevision uint64
	Metrics         map[string]MetricData
	NumaMetrics     map[int]map[string]MetricData
}

// SetPodMetrics replaces one sample, including failures, so a failed collection
// cannot leave a successful previous sample available to new readers. The maps
// belong to the store after this call and must not be modified by the caller.
func (c *MetricStore) SetPodMetrics(podUID string, data PodMetricData) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if current, ok := c.podMetricMap[podUID]; ok && current.MappingRevision > data.MappingRevision {
		return
	}
	c.podMetricMap[podUID] = data
}

func (c *MetricStore) GetPodMetric(podUID, cgroupPath string, revision uint64, metricName string, numaID int) (MetricData, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	sample, ok := c.podMetricMap[podUID]
	if !ok || sample.CgroupPath != cgroupPath || sample.MappingRevision != revision {
		return MetricData{}, fmt.Errorf("pod %s metric mapping is unavailable", podUID)
	}
	values := sample.Metrics
	if numaID >= 0 {
		values = sample.NumaMetrics[numaID]
	}
	data, ok := values[metricName]
	if !ok {
		return MetricData{}, fmt.Errorf("pod %s metric %s numa %d is unavailable", podUID, metricName, numaID)
	}
	return data, nil
}

// GCPodMetricSamples uses a successful complete source inventory, not the collection
// success set. It does not touch container or shared cgroup metrics.
func (c *MetricStore) GCPodMetricSamples(livingPodUIDSet map[string]bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for uid := range c.podMetricMap {
		if !livingPodUIDSet[uid] {
			delete(c.podMetricMap, uid)
		}
	}
}
