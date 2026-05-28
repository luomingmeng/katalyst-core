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

package cpuweight

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	resourceutil "k8s.io/kubernetes/pkg/api/v1/resource"

	"github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/adminqos/finegrainedresource"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	cgroupmgr "github.com/kubewharf/katalyst-core/pkg/util/cgroup/manager"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/native"
)

const (
	maxCPUShares   = 262144
	minCPUShares   = 2
	sharesPerCPU   = 1024
	milliCPUPerCPU = 1000
)

type Manager interface {
	UpdateCPUWeight(dynamicConfig *dynamic.DynamicAgentConfiguration) error
}

type managerImpl struct {
	metaServer *metaserver.MetaServer
}

var (
	instance                 *managerImpl
	once                     sync.Once
	DemandCoresAnnotationKey = consts.PodAnnotationCPUWeightDemandCoresKey
	BurstRatioAnnotationKey  = consts.PodAnnotationCPUWeightBurstRatioKey
)

func GetManager(metaServer *metaserver.MetaServer) Manager {
	once.Do(func() {
		instance = newManager(metaServer)
	})
	return instance
}

func newManager(metaServer *metaserver.MetaServer) *managerImpl {
	return &managerImpl{
		metaServer: metaServer,
	}
}

func (m *managerImpl) UpdateCPUWeight(dynamicConfig *dynamic.DynamicAgentConfiguration) error {
	if dynamicConfig == nil {
		return nil
	}
	dynamicConf := dynamicConfig.GetDynamicConfiguration()
	if dynamicConf == nil || dynamicConf.AdminQoSConfiguration == nil ||
		dynamicConf.AdminQoSConfiguration.FineGrainedResourceConfiguration == nil {
		return nil
	}
	config := dynamicConf.AdminQoSConfiguration.FineGrainedResourceConfiguration.CPUWeightConfiguration
	if config == nil {
		return nil
	}

	errList := make([]error, 0)

	for _, rule := range config.RestoreRules {
		if err := m.processRestoreRule(rule); err != nil {
			errList = append(errList, fmt.Errorf("failed to process restore rule %s: %w", rule.Name, err))
		}
	}

	for _, rule := range config.OverrideRules {
		if err := m.processOverrideRule(rule); err != nil {
			errList = append(errList, fmt.Errorf("failed to process override rule %s: %w", rule.Name, err))
		}
	}

	return utilerrors.NewAggregate(errList)
}

func (m *managerImpl) processRestoreRule(rule finegrainedresource.CPUWeightRestore) error {
	if rule.PodSelector == "" {
		return fmt.Errorf("empty pod selector")
	}
	podSelector, err := labels.Parse(rule.PodSelector)
	if err != nil {
		return fmt.Errorf("failed to parse pod selector: %w", err)
	}
	pods, err := m.metaServer.GetPodList(context.Background(), func(pod *v1.Pod) bool {
		return podSelector.Matches(labels.Set(pod.Labels)) && native.PodIsActive(pod)
	})
	if err != nil {
		return fmt.Errorf("failed to get pods: %w", err)
	}
	if len(pods) == 0 {
		return nil
	}

	var errList []error
	for _, pod := range pods {
		cpuRequests := m.getPodOriginalCPURequests(pod)
		cpuShares := milliCPUToShares(cpuRequests)

		absCgroupPath, err := common.GetPodAbsCgroupPath(common.CgroupSubsysCPU, string(pod.UID))
		if err != nil {
			errList = append(errList, fmt.Errorf("failed to get the absolute path of pod %s: %w", pod.Name, err))
			continue
		}

		cpuData := &common.CPUData{Shares: cpuShares}
		if err := cgroupmgr.ApplyCPUWithAbsolutePath(absCgroupPath, cpuData); err != nil {
			errList = append(errList, fmt.Errorf("failed to apply cpu weight for pod %s: %w", pod.Name, err))
			continue
		}
	}

	return utilerrors.NewAggregate(errList)
}

func (m *managerImpl) processOverrideRule(rule finegrainedresource.CPUWeightOverride) error {
	if rule.PodSelector == "" {
		return fmt.Errorf("empty pod selector")
	}
	podSelector, err := labels.Parse(rule.PodSelector)
	if err != nil {
		return fmt.Errorf("failed to parse pod selector: %w", err)
	}
	pods, err := m.metaServer.GetPodList(context.Background(), func(pod *v1.Pod) bool {
		return podSelector.Matches(labels.Set(pod.Labels)) && native.PodIsActive(pod)
	})
	if err != nil {
		return fmt.Errorf("failed to get pods: %w", err)
	}
	if len(pods) == 0 {
		return nil
	}

	cpuRequests := rule.PodCPUDemand * milliCPUPerCPU
	cpuShares := milliCPUToShares(cpuRequests)

	var errList []error
	for _, pod := range pods {
		absCgroupPath, err := common.GetPodAbsCgroupPath(common.CgroupSubsysCPU, string(pod.UID))
		if err != nil {
			errList = append(errList, fmt.Errorf("failed to get the absolute path of pod %s: %w", pod.Name, err))
			continue
		}

		cpuData := &common.CPUData{Shares: cpuShares}
		if err := cgroupmgr.ApplyCPUWithAbsolutePath(absCgroupPath, cpuData); err != nil {
			errList = append(errList, fmt.Errorf("failed to apply cpu weight for pod %s: %w", pod.Name, err))
			continue
		}
	}

	return utilerrors.NewAggregate(errList)
}

func (m *managerImpl) getPodOriginalCPURequests(pod *v1.Pod) int64 {
	if cpuRequests := m.getMilliCPUFromAnnotation(pod); cpuRequests > 0 {
		return cpuRequests
	}

	reqs, _ := resourceutil.PodRequestsAndLimits(pod)
	if cpuQuantity, ok := reqs[v1.ResourceCPU]; ok {
		return cpuQuantity.MilliValue()
	}

	return 0
}

func (m *managerImpl) getMilliCPUFromAnnotation(pod *v1.Pod) int64 {
	if pod.Annotations == nil {
		return 0
	}

	value, ok := pod.Annotations[DemandCoresAnnotationKey]
	if !ok || value == "" {
		return 0
	}

	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		general.Warningf("failed to parse cpu count from annotation %s=%s: %v",
			DemandCoresAnnotationKey, value, err)
		return 0
	}

	burstRatio := 1.0
	if ratioStr, ok := pod.Annotations[BurstRatioAnnotationKey]; ok {
		annoRatio, err := strconv.ParseFloat(ratioStr, 64)
		if err != nil || annoRatio <= 0 {
			general.Warningf("failed to parse cpu weight burst ratio from annotation %s=%s: %v",
				BurstRatioAnnotationKey, ratioStr, err)
		} else {
			burstRatio = annoRatio
		}
	}

	return int64(float64(quantity.MilliValue()) * burstRatio)
}

func milliCPUToShares(milliCPU int64) uint64 {
	shares := milliCPU * sharesPerCPU / milliCPUPerCPU
	if shares < minCPUShares {
		shares = minCPUShares
	}
	if shares > maxCPUShares {
		shares = maxCPUShares
	}
	return uint64(shares)
}
