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
	"context"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"

	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

// supportedPodMetrics is read-only after initialization. A convertible name or
// a stored value does not imply whole-workload support; add keys only after the
// corresponding collection and semantics are supported.
// TODO: Refactor capability declarations to avoid maintaining a separate metric
// allowlist while preserving unsupported versus unavailable semantics.
var supportedPodMetrics = map[string]struct{}{
	consts.MetricCPUUsageCgroup:             {},
	consts.MetricCPUUsageUserCgroup:         {},
	consts.MetricCPUUsageSysCgroup:          {},
	consts.MetricCPUNrRunnableCgroup:        {},
	consts.MetricCPUNrUninterruptibleCgroup: {},
	consts.MetricCPUNrIOWaitCgroup:          {},
	consts.MetricLoad1MinCgroup:             {},
	consts.MetricLoad5MinCgroup:             {},
	consts.MetricLoad15MinCgroup:            {},
	consts.MetricMemUsageCgroup:             {},
	consts.MetricMemRssCgroup:               {},
	consts.MetricMemCacheCgroup:             {},
	consts.MetricMemShmemCgroup:             {},
	consts.MetricsMemTotalPerNumaCgroup:     {},
	consts.MetricsMemFilePerNumaCgroup:      {},
	consts.MetricsMemAnonPerNumaCgroup:      {},
}

func (f *MetricsFetcherImpl) GetPodMetric(p *v1.Pod, metricName string, numaID int) (utilmetric.MetricData, bool, error) {
	provider, ok := f.podFetcher.(types.PodMetricTargetProvider)
	if !ok {
		return utilmetric.MetricData{}, false, nil
	}
	return getPodMetric(provider, f.metricStore, f.checkMetricDataExpire, p, metricName, numaID)
}

// Real and fake fetchers share mapping, expiry and recheck semantics. The
// provider, not the presence of a stored sample, determines target ownership.
func getPodMetric(provider types.PodMetricTargetProvider, store *utilmetric.MetricStore, checkExpire CheckMetricDataExpireFunc, p *v1.Pod, metricName string, numaID int) (utilmetric.MetricData, bool, error) {
	if provider == nil {
		return utilmetric.MetricData{}, false, nil
	}
	target, handled, err := provider.ResolvePodMetricTarget(context.Background(), p)
	if !handled || err != nil {
		return utilmetric.MetricData{}, handled, err
	}
	mappedName := metricName
	if strings.HasSuffix(mappedName, consts.MetricContainerSuffix) {
		mappedName = strings.TrimSuffix(mappedName, consts.MetricContainerSuffix) + consts.MetricCgroupSuffix
	}
	if _, ok := supportedPodMetrics[mappedName]; !ok {
		return utilmetric.MetricData{}, true, fmt.Errorf("%w: %s", utilmetric.ErrPodMetricUnsupported, metricName)
	}
	data, err := checkExpire(store.GetPodMetric(string(p.UID), target.CgroupPath, target.MappingRevision, mappedName, numaID))
	if err != nil {
		return data, true, err
	}
	current, handled, err := provider.ResolvePodMetricTarget(context.Background(), p)
	if err != nil || !handled || !target.SameMapping(current) {
		return utilmetric.MetricData{}, true, fmt.Errorf("pod metric mapping changed while reading: %v", err)
	}
	return data, true, nil
}
