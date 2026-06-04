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

package allocation

import (
	"fmt"
	"time"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/strategy/allocate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/util"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

// allocationLogContext extracts pod and resource identifiers from the
// allocation context for use in log messages. Safe to call with a nil ctx.
func allocationLogContext(ctx *allocate.AllocationContext) (podInfo, resourceName string) {
	var podNamespace, podName, containerName string
	if ctx != nil {
		if ctx.ResourceReq != nil {
			podNamespace = ctx.ResourceReq.PodNamespace
			podName = ctx.ResourceReq.PodName
			containerName = ctx.ResourceReq.ContainerName
		}
		if ctx.DeviceReq != nil {
			resourceName = ctx.DeviceReq.DeviceName
		}
	}
	podInfo = fmt.Sprintf("pod: %s/%s, container: %s", podNamespace, podName, containerName)
	return
}

// emitDuration emits the elapsed time since startTime as a raw float64
// millisecond metric and logs it. Safe to call with a nil emitter.
func emitDuration(emitter metrics.MetricEmitter, metricName string,
	startTime time.Time, tags ...metrics.MetricTag,
) {
	elapsed := time.Since(startTime)
	if emitter != nil {
		_ = emitter.StoreFloat64(metricName, float64(elapsed)/float64(time.Millisecond),
			metrics.MetricTypeNameRaw, tags...)
	}
	general.InfoS("finished", "metric", metricName, "duration", elapsed)
}

// timedFilteringStrategy wraps a FilteringStrategy and emits its Filter duration.
type timedFilteringStrategy struct {
	allocate.FilteringStrategy
	emitter    metrics.MetricEmitter
	metricName string
	tags       []metrics.MetricTag
}

// Filter overrides the promoted Filter() to add timing/metrics.
func (t *timedFilteringStrategy) Filter(ctx *allocate.AllocationContext, devices []string) ([]string, error) {
	start := time.Now()
	strategyName := t.FilteringStrategy.Name()
	tags := append(t.tags, metrics.MetricTag{Key: "strategyName", Val: strategyName})
	defer emitDuration(t.emitter, t.metricName, start, tags...)

	podInfo, resourceName := allocationLogContext(ctx)
	filtered, err := t.FilteringStrategy.Filter(ctx, devices)
	if err != nil {
		general.Errorf("%s, failed to filter available devices with strategy %s, resource %s, err: %v",
			podInfo, strategyName, resourceName, err)
		return filtered, err
	}
	general.Infof("%s, success filter %s, resource %s, available devices %v -> %v",
		podInfo, strategyName, resourceName, devices, filtered)
	return filtered, nil
}

// timedSortingStrategy wraps a SortingStrategy and emits its Sort duration.
type timedSortingStrategy struct {
	allocate.SortingStrategy
	emitter    metrics.MetricEmitter
	metricName string
	tags       []metrics.MetricTag
}

func (t *timedSortingStrategy) Sort(ctx *allocate.AllocationContext, devices []string) ([]string, error) {
	start := time.Now()
	strategyName := t.SortingStrategy.Name()
	tags := append(t.tags, metrics.MetricTag{Key: "strategyName", Val: strategyName})
	defer emitDuration(t.emitter, t.metricName, start, tags...)

	podInfo, resourceName := allocationLogContext(ctx)
	sorted, err := t.SortingStrategy.Sort(ctx, devices)
	if err != nil {
		general.Errorf("%s, failed to sort available devices with strategy %s, resource %s, err: %v",
			podInfo, strategyName, resourceName, err)
		return sorted, err
	}
	general.Infof("%s, success sort available devices with strategy %s, resource %s: %v",
		podInfo, strategyName, resourceName, sorted)
	return sorted, nil
}

// timedBindingStrategy wraps a BindingStrategy and emits its Bind duration.
type timedBindingStrategy struct {
	allocate.BindingStrategy
	emitter    metrics.MetricEmitter
	metricName string
	tags       []metrics.MetricTag
}

func (t *timedBindingStrategy) Bind(ctx *allocate.AllocationContext, devices []string) (*allocate.AllocationResult, error) {
	start := time.Now()
	strategyName := t.BindingStrategy.Name()
	tags := append(t.tags, metrics.MetricTag{Key: "strategyName", Val: strategyName})
	defer emitDuration(t.emitter, t.metricName, start, tags...)

	podInfo, resourceName := allocationLogContext(ctx)
	result, err := t.BindingStrategy.Bind(ctx, devices)
	if err != nil {
		general.Errorf("%s, failed to bind available devices with strategy %s, resource %s, err: %v",
			podInfo, strategyName, resourceName, err)
		return result, err
	}
	var allocated []string
	if result != nil {
		allocated = result.AllocatedDevices
	}
	general.Infof("%s, success bind available devices with strategy %s, resource %s: %v",
		podInfo, strategyName, resourceName, allocated)
	return result, nil
}

// withFilterTiming returns a FilteringStrategy that emits filter duration metrics.
func withFilterTiming(fs allocate.FilteringStrategy, emitter metrics.MetricEmitter, tags ...metrics.MetricTag) allocate.FilteringStrategy {
	return &timedFilteringStrategy{
		FilteringStrategy: fs,
		emitter:           emitter,
		metricName:        util.MetricNameAllocateFilterDuration,
		tags:              tags,
	}
}

// withSortTiming returns a SortingStrategy that emits sort duration metrics.
func withSortTiming(ss allocate.SortingStrategy, emitter metrics.MetricEmitter, tags ...metrics.MetricTag) allocate.SortingStrategy {
	return &timedSortingStrategy{
		SortingStrategy: ss,
		emitter:         emitter,
		metricName:      util.MetricNameAllocateSortDuration,
		tags:            tags,
	}
}

// withBindTiming returns a BindingStrategy that emits bind duration metrics.
func withBindTiming(bs allocate.BindingStrategy, emitter metrics.MetricEmitter, tags ...metrics.MetricTag) allocate.BindingStrategy {
	return &timedBindingStrategy{
		BindingStrategy: bs,
		emitter:         emitter,
		metricName:      util.MetricNameAllocateBindDuration,
		tags:            tags,
	}
}
