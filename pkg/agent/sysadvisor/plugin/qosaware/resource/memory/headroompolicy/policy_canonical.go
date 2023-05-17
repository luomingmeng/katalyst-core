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

package headroompolicy

import (
	"context"
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"

	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/metacache"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/plugin/qosaware/resource/helper"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/sysadvisor/qosaware/resource/memory/headroom"
	"github.com/kubewharf/katalyst-core/pkg/config/generic"
	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/metric"
)

type PolicyCanonical struct {
	*PolicyBase

	// memoryHeadroom is valid to be used iff updateStatus successes
	memoryHeadroom               float64
	updateStatus                 types.PolicyUpdateStatus
	qosConfig                    *generic.QoSConfiguration
	policyCanonicalConfiguration *headroom.MemoryPolicyCanonicalConfiguration
}

func NewPolicyCanonical(conf *config.Configuration, _ interface{}, metaReader metacache.MetaReader,
	metaServer *metaserver.MetaServer, _ metrics.MetricEmitter) HeadroomPolicy {
	p := PolicyCanonical{
		PolicyBase:                   NewPolicyBase(metaReader, metaServer),
		updateStatus:                 types.PolicyUpdateFailed,
		qosConfig:                    conf.QoSConfiguration,
		policyCanonicalConfiguration: conf.MemoryHeadroomPolicyConfiguration.MemoryPolicyCanonicalConfiguration,
	}

	return &p
}

func (p *PolicyCanonical) estimateNonReclaimedQoSMemoryRequirement() (float64, error) {
	var (
		memoryEstimation float64 = 0
		containerCnt     float64 = 0
		errList          []error
	)

	f := func(podUID string, containerName string, ci *types.ContainerInfo) bool {
		containerEstimation, err := helper.EstimateContainerMemoryUsage(ci, p.metaReader)
		if err != nil {
			errList = append(errList, err)
			return true
		}

		general.Infof("pod %v container %v estimation %.2e", ci.PodName, containerName, containerEstimation)
		memoryEstimation += containerEstimation
		containerCnt += 1
		return true
	}
	p.metaReader.RangeContainer(f)
	general.Infof("memory requirement estimation: %.2e, #container %v", memoryEstimation, containerCnt)

	return memoryEstimation, errors.NewAggregate(errList)
}

func (p *PolicyCanonical) Update() (err error) {
	defer func() {
		if err != nil {
			p.updateStatus = types.PolicyUpdateFailed
		} else {
			p.updateStatus = types.PolicyUpdateSucceeded
		}
	}()

	var (
		memoryEstimateRequirement float64
		memoryBuffer              float64
	)

	maxAllocatableMemory := float64(p.essentials.Total - p.essentials.ReservedForAllocate)
	memoryEstimateRequirement, err = p.estimateNonReclaimedQoSMemoryRequirement()
	if err != nil {
		return err
	}
	memoryHeadroomWithoutBuffer := math.Max(maxAllocatableMemory-memoryEstimateRequirement, 0)

	if p.policyCanonicalConfiguration.EnableBuffer {
		memoryBuffer, err = p.calculateMemoryBuffer(memoryEstimateRequirement)
		if err != nil {
			return err
		}
	}

	p.memoryHeadroom = math.Max(memoryHeadroomWithoutBuffer+memoryBuffer, 0)
	p.memoryHeadroom = math.Min(p.memoryHeadroom, maxAllocatableMemory)
	general.Infof("without buffer memory headroom: %.2e, final memory headroom: %.2e, memory buffer: %.2e, max memory allocatable: %.2e",
		memoryHeadroomWithoutBuffer, p.memoryHeadroom, memoryBuffer, maxAllocatableMemory)

	return nil
}

func (p *PolicyCanonical) GetHeadroom() (resource.Quantity, error) {
	if p.updateStatus != types.PolicyUpdateSucceeded {
		return resource.Quantity{}, fmt.Errorf("last update failed")
	}

	return *resource.NewQuantity(int64(p.memoryHeadroom), resource.BinarySI), nil
}

func (p *PolicyCanonical) calculateMemoryBuffer(estimateNonReclaimedRequirement float64) (float64, error) {
	var (
		systemMetrics               *systemMemoryMetrics
		systemQoSMetrics            *regionMemoryMetrics
		sharedAndDedicateQoSMetrics *regionMemoryMetrics
		nodeMemoryCPURatio          float64
		err                         error
	)

	systemMetrics, err = p.getSystemMemoryInfo()
	if err != nil {
		return 0, err
	}

	sharedAndDedicateQoSMetrics, err = p.getRegionMemoryMetrics(p.filterSharedAndDedicateQoSPods)
	if err != nil {
		return 0, err
	}

	systemQoSMetrics, err = p.getRegionMemoryMetrics(p.filterSystemQoSPods)
	if err != nil {
		return 0, err
	}

	nodeMemoryCPURatio, err = p.getNodeMemoryCPURatio()
	if err != nil {
		return 0, err
	}

	// calculate system buffer with double scale_factor to make kswapd less happened
	systemFactor := systemMetrics.memoryTotal * 2 * systemMetrics.scaleFactor / 10000
	systemBuffer := systemMetrics.memoryFree*p.policyCanonicalConfiguration.MemoryFreeEstimationUtilization - systemFactor

	// calculate shared and dedicate qos total used
	sharedAndDedicateQoSTotalUsed := sharedAndDedicateQoSMetrics.rss + sharedAndDedicateQoSMetrics.shmem +
		sharedAndDedicateQoSMetrics.cache

	// calculate system qos extra usage with only shmem and cache
	systemQoSExtraUsage := systemQoSMetrics.cache + systemQoSMetrics.shmem

	// calculate non-reclaimed qos buffer
	nonReclaimedQoSExtraUsage := estimateNonReclaimedRequirement - sharedAndDedicateQoSTotalUsed + systemQoSExtraUsage
	buffer := math.Max(systemBuffer-nonReclaimedQoSExtraUsage, 0)

	// calculate cache oversold buffer if cache oversold rate is set and memory/cpu ratio is in range and memory utilization is not 0
	if p.policyCanonicalConfiguration.CacheOversoldRate > 0 && nodeMemoryCPURatio > p.policyCanonicalConfiguration.CacheOversoldMemoryCPURatioLowerBound &&
		nodeMemoryCPURatio < p.policyCanonicalConfiguration.CacheOversoldMemoryCPURatioUpperBound && systemMetrics.memoryUtilization > 0 {
		cacheBuffer := (systemMetrics.memoryTotal*(1-systemMetrics.memoryUtilization) - systemMetrics.memoryFree) *
			p.policyCanonicalConfiguration.CacheOversoldRate
		buffer = math.Max(buffer, cacheBuffer)
		general.Infof("cache oversold rate: %.2f, cache buffer: %.2e, buffer: %.2e",
			p.policyCanonicalConfiguration.CacheOversoldRate, cacheBuffer, buffer)
	}

	result := buffer + p.policyCanonicalConfiguration.StaticMemoryOversold
	general.Infof("system buffer: %.2e, non-reclaimed QoS extra usage: %.2e, static oversold: %.2e, result: %.2e",
		systemBuffer, nonReclaimedQoSExtraUsage, p.policyCanonicalConfiguration.StaticMemoryOversold, result)
	return result, nil
}

type systemMemoryMetrics struct {
	memoryTotal       float64
	memoryFree        float64
	scaleFactor       float64
	memoryUtilization float64
}

func (p *PolicyCanonical) getSystemMemoryInfo() (*systemMemoryMetrics, error) {
	var (
		info systemMemoryMetrics
		err  error
	)

	info.memoryTotal, err = p.metaServer.GetNodeMetric(consts.MetricMemTotalSystem)
	if err != nil {
		return nil, err
	}

	info.memoryFree, err = p.metaServer.GetNodeMetric(consts.MetricMemFreeSystem)
	if err != nil {
		return nil, err
	}

	info.scaleFactor, err = p.metaServer.GetNodeMetric(consts.MetricMemScaleFactorSystem)
	if err != nil {
		return nil, err
	}

	used, err := p.metaServer.GetNodeMetric(consts.MetricMemUsedSystem)
	if err != nil {
		return nil, err
	}
	info.memoryUtilization = used / info.memoryTotal

	return &info, nil
}

func (p *PolicyCanonical) getNodeMemoryCPURatio() (float64, error) {
	cpuCapacity := p.metaServer.MachineInfo.NumCores
	memoryCapacity := p.metaServer.MemoryCapacity
	if cpuCapacity == 0 {
		return 0, fmt.Errorf("cpu capacity is 0")
	}

	return float64(memoryCapacity) / 1024 / 1024 / 1024 / float64(cpuCapacity), nil
}

type regionMemoryMetrics struct {
	cache float64
	shmem float64
	rss   float64
}

func (p *PolicyCanonical) getRegionMemoryMetrics(filter func(pod *v1.Pod) bool) (*regionMemoryMetrics, error) {
	regionPods, err := p.metaServer.GetPodList(context.Background(), filter)
	if err != nil {
		return nil, err
	}

	cache := p.metaServer.AggregatePodMetric(regionPods, consts.MetricMemCacheContainer, metric.AggregatorSum, metric.DefaultContainerMetricFilter)
	shmem := p.metaServer.AggregatePodMetric(regionPods, consts.MetricMemShmemContainer, metric.AggregatorSum, metric.DefaultContainerMetricFilter)
	rss := p.metaServer.AggregatePodMetric(regionPods, consts.MetricMemRssContainer, metric.AggregatorSum, metric.DefaultContainerMetricFilter)
	return &regionMemoryMetrics{
		cache: cache,
		shmem: shmem,
		rss:   rss,
	}, nil
}

func (p *PolicyCanonical) filterSystemQoSPods(pod *v1.Pod) bool {
	if ok, err := p.qosConfig.CheckSystemQoSForPod(pod); err != nil {
		klog.Errorf("filter pod %v err: %v", pod.Name, err)
		return false
	} else {
		return ok
	}
}

func (p *PolicyCanonical) filterSharedAndDedicateQoSPods(pod *v1.Pod) bool {
	isSharedQoS, err := p.qosConfig.CheckSharedQoSForPod(pod)
	if err != nil {
		klog.Errorf("filter pod %v err: %v", pod.Name, err)
		return false
	}

	isDedicateQoS, err := p.qosConfig.CheckDedicatedQoSForPod(pod)
	if err != nil {
		klog.Errorf("filter pod %v err: %v", pod.Name, err)
		return false
	}

	return isSharedQoS || isDedicateQoS
}
