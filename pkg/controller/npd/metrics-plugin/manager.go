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

package metrics_plugin

import (
	"sync"

	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"github.com/kubewharf/katalyst-api/pkg/apis/node/v1alpha1"
	"github.com/kubewharf/katalyst-core/pkg/util"
)

// MetricsUpdater is used by MetricsPlugin as a unified implementation
// to trigger metrics updating logic.
type MetricsUpdater interface {
	UpdateNodeMetrics(name string, scopedNodeMetrics []v1alpha1.ScopedNodeMetrics)
	UpdatePodMetrics(nodeName string, scopedPodMetrics []v1alpha1.ScopedPodMetrics)
}

// MetricsGetter is used by npd controller as metrics notifier to trigger
// update real npd.
type MetricsGetter interface {
	GetNodeProfileStatusQueue() workqueue.RateLimitingInterface
	GetNodeProfileStatus(name string) *v1alpha1.NodeProfileDescriptorStatus
	DeleteNodeProfileStatus(name string)
}

type MetricsManager struct {
	sync.Mutex

	statusQueue workqueue.RateLimitingInterface
	statusMap   map[string]*v1alpha1.NodeProfileDescriptorStatus
}

var (
	_ MetricsUpdater = &MetricsManager{}
	_ MetricsGetter  = &MetricsManager{}
)

func NewMetricsManager() *MetricsManager {
	return &MetricsManager{
		statusMap:   make(map[string]*v1alpha1.NodeProfileDescriptorStatus),
		statusQueue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "npd"),
	}
}

func (im *MetricsManager) UpdateNodeMetrics(name string, scopedNodeMetrics []v1alpha1.ScopedNodeMetrics) {
	im.Lock()

	if _, ok := im.statusMap[name]; !ok {
		im.statusMap[name] = initNodeProfileDescriptorStatus()
	}
	for _, scopedNodeMetric := range scopedNodeMetrics {
		util.InsertNPDScopedNodeMetrics(im.statusMap[name], &scopedNodeMetric)
	}

	im.Unlock()

	im.statusQueue.AddRateLimited(name)
}

func (im *MetricsManager) UpdatePodMetrics(nodeName string, scopedPodMetrics []v1alpha1.ScopedPodMetrics) {
	im.Lock()

	if _, ok := im.statusMap[nodeName]; !ok {
		im.statusMap[nodeName] = initNodeProfileDescriptorStatus()
	}
	for _, scopedPodMetric := range scopedPodMetrics {
		util.InsertNPDScopedPodMetrics(im.statusMap[nodeName], &scopedPodMetric)
	}

	im.Unlock()

	im.statusQueue.AddRateLimited(nodeName)
}

func (im *MetricsManager) GetNodeProfileStatusQueue() workqueue.RateLimitingInterface {
	return im.statusQueue
}

func (im *MetricsManager) GetNodeProfileStatus(name string) *v1alpha1.NodeProfileDescriptorStatus {
	im.Lock()
	defer im.Unlock()

	status, ok := im.statusMap[name]
	if !ok {
		klog.Warningf("npd status doesn't exist for node: %v", name)
		return nil
	}
	return status
}

func (im *MetricsManager) DeleteNodeProfileStatus(name string) {
	im.Lock()
	defer im.Unlock()

	delete(im.statusMap, name)
}

func initNodeProfileDescriptorStatus() *v1alpha1.NodeProfileDescriptorStatus {
	return &v1alpha1.NodeProfileDescriptorStatus{
		NodeMetrics: []v1alpha1.ScopedNodeMetrics{},
		PodMetrics:  []v1alpha1.ScopedPodMetrics{},
	}
}
