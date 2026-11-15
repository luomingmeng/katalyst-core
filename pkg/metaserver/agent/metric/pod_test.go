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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

type wholePodFetcher struct {
	pod.PodFetcherStub
	target    types.PodMetricTarget
	err       error
	onResolve func()
}

func (f *wholePodFetcher) GetPodMetricTargets(context.Context) ([]types.PodMetricTarget, error) {
	return []types.PodMetricTarget{f.target}, f.err
}

func (f *wholePodFetcher) ResolvePodMetricTarget(_ context.Context, p *v1.Pod) (types.PodMetricTarget, bool, error) {
	if p == nil || p.UID != f.target.Pod.UID {
		return types.PodMetricTarget{}, false, nil
	}
	if f.onResolve != nil {
		f.onResolve()
	}
	return f.target, true, f.err
}

func TestPodMetricTargetSameMapping(t *testing.T) {
	t.Parallel()
	target := types.PodMetricTarget{Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "target", Name: "workload", Namespace: "default"}}, CgroupPath: "/target", MappingRevision: 1}
	for _, mode := range []string{"same", "nil receiver pod", "nil other pod", "empty path", "path", "revision", "uid", "name", "namespace", "runtime", "nil versus empty runtime", "same runtime value"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			a, b := target, target
			a.Pod, b.Pod = target.Pod.DeepCopy(), target.Pod.DeepCopy()
			kata, other, empty := "kata", "other", ""
			switch mode {
			case "nil receiver pod":
				a.Pod = nil
			case "nil other pod":
				b.Pod = nil
			case "empty path":
				a.CgroupPath, b.CgroupPath = "", ""
			case "path":
				b.CgroupPath = "/other"
			case "revision":
				b.MappingRevision++
			case "uid":
				b.Pod.UID = "other"
			case "name":
				b.Pod.Name = "other"
			case "namespace":
				b.Pod.Namespace = "other"
			case "runtime":
				a.Pod.Spec.RuntimeClassName, b.Pod.Spec.RuntimeClassName = &kata, &other
			case "nil versus empty runtime":
				b.Pod.Spec.RuntimeClassName = &empty
			case "same runtime value":
				a.Pod.Spec.RuntimeClassName = &kata
				b.Pod = a.Pod.DeepCopy()
			}
			assert.Equal(t, mode == "same" || mode == "same runtime value", a.SameMapping(b))
		})
	}
}

func TestGetPodMetricRechecksTargetMapping(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"path", "revision", "uid", "name", "namespace", "runtime", "nil pod"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			target := types.PodMetricTarget{Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "target", Name: "workload", Namespace: "default"}}, CgroupPath: "/target", MappingRevision: 1}
			provider := &wholePodFetcher{target: target}
			store := utilmetric.NewMetricStore()
			store.SetPodMetrics("target", utilmetric.PodMetricData{CgroupPath: "/target", MappingRevision: 1, Metrics: map[string]utilmetric.MetricData{consts.MetricMemRssCgroup: {Value: 7}}})
			calls := 0
			provider.onResolve = func() {
				calls++
				if calls != 2 {
					return
				}
				provider.target.Pod = target.Pod.DeepCopy()
				switch mode {
				case "path":
					provider.target.CgroupPath = "/other"
				case "revision":
					provider.target.MappingRevision++
				case "uid":
					provider.target.Pod.UID = "other"
				case "name":
					provider.target.Pod.Name = "other"
				case "namespace":
					provider.target.Pod.Namespace = "other"
				case "runtime":
					runtime := "kata"
					provider.target.Pod.Spec.RuntimeClassName = &runtime
				case "nil pod":
					provider.target.Pod = nil
				}
			}
			f := &MetricsFetcherImpl{podFetcher: provider, metricStore: store, checkMetricDataExpire: checkMetricDataExpireFunc(0)}
			data, handled, err := f.GetPodMetric(target.Pod, consts.MetricMemRssContainer, -1)
			require.Error(t, err)
			assert.True(t, handled)
			assert.Equal(t, utilmetric.MetricData{}, data)
			assert.Equal(t, 2, calls)
		})
	}
}

func TestFakeGetPodMetricWithTargetProvider(t *testing.T) {
	t.Parallel()
	target := types.PodMetricTarget{Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "target", Name: "workload", Namespace: "default"}}, CgroupPath: "/target", MappingRevision: 1}
	provider := &wholePodFetcher{target: target}
	f := NewFakeMetricsFetcher(metrics.DummyMetrics{}).(*FakeMetricsFetcher)
	f.SetPodMetricTargetProvider(provider)
	now := time.Now()
	f.RegisterExternalMetric(func(store *utilmetric.MetricStore) {
		store.SetPodMetrics("target", utilmetric.PodMetricData{
			CgroupPath: "/target", MappingRevision: 1,
			Metrics:     map[string]utilmetric.MetricData{consts.MetricMemRssCgroup: {Value: 7, Time: &now}},
			NumaMetrics: map[int]map[string]utilmetric.MetricData{0: {consts.MetricsMemTotalPerNumaCgroup: {Value: 9, Time: &now}}},
		})
	})
	f.Run(context.Background())
	data, handled, err := f.GetPodMetric(target.Pod, consts.MetricMemRssContainer, -1)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, float64(7), data.Value)
	data, handled, err = f.GetPodMetric(target.Pod, consts.MetricsMemTotalPerNumaContainer, 0)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, float64(9), data.Value)
}

func TestFakePodMetricFailuresAndFallback(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"no provider", "ordinary", "nil pod", "unsupported", "missing sample", "missing scalar", "missing NUMA", "expired scalar", "expired NUMA", "wrong revision", "wrong path", "source failure", "changed mapping"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			target := types.PodMetricTarget{Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "target"}}, CgroupPath: "/target", MappingRevision: 1}
			provider := &wholePodFetcher{target: target}
			f := NewFakeMetricsFetcher(metrics.DummyMetrics{}).(*FakeMetricsFetcher)
			f.SetPodMetricTargetProvider(provider)
			now := time.Now()
			old := now.Add(-2 * time.Minute)
			sample := utilmetric.PodMetricData{
				CgroupPath: "/target", MappingRevision: 1,
				Metrics:     map[string]utilmetric.MetricData{consts.MetricMemRssCgroup: {Value: 7, Time: &now}},
				NumaMetrics: map[int]map[string]utilmetric.MetricData{0: {consts.MetricsMemTotalPerNumaCgroup: {Value: 9, Time: &now}}},
			}
			p, name, numaID := target.Pod, consts.MetricMemRssContainer, -1
			switch mode {
			case "no provider":
				f.SetPodMetricTargetProvider(nil)
			case "ordinary":
				p = &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "ordinary"}}
			case "nil pod":
				p = nil
			case "unsupported":
				name = consts.MetricMemLimitCgroup
			case "missing scalar":
				sample.Metrics = nil
			case "missing NUMA":
				name, numaID, sample.NumaMetrics = consts.MetricsMemTotalPerNumaContainer, 0, nil
			case "expired scalar":
				sample.Metrics[consts.MetricMemRssCgroup] = utilmetric.MetricData{Value: 7, Time: &old}
			case "expired NUMA":
				name, numaID = consts.MetricsMemTotalPerNumaContainer, 0
				sample.NumaMetrics[0][consts.MetricsMemTotalPerNumaCgroup] = utilmetric.MetricData{Value: 9, Time: &old}
			case "wrong revision":
				sample.MappingRevision = 2
			case "wrong path":
				sample.CgroupPath = "/other"
			case "source failure":
				provider.err = errors.New("source unavailable")
			}
			f.RegisterExternalMetric(func(store *utilmetric.MetricStore) {
				if mode != "missing sample" {
					store.SetPodMetrics("target", sample)
				}
				store.SetPodMetrics("ordinary", sample) // a sample alone cannot select ownership
			})
			f.Run(context.Background())
			calls := 0
			provider.onResolve = func() {
				calls++
				if mode == "changed mapping" && calls == 2 {
					provider.target.MappingRevision++
				}
			}
			data, handled, err := f.GetPodMetric(p, name, numaID)
			if mode == "no provider" || mode == "ordinary" || mode == "nil pod" {
				require.NoError(t, err)
				assert.False(t, handled)
				assert.Equal(t, utilmetric.MetricData{}, data)
				return
			}
			assert.True(t, handled)
			require.Error(t, err)
			switch mode {
			case "unsupported":
				assert.ErrorIs(t, err, utilmetric.ErrPodMetricUnsupported)
			case "expired scalar", "expired NUMA":
				assert.True(t, IsMetricDataExpired(err))
			case "source failure":
				assert.ErrorIs(t, err, provider.err)
			case "changed mapping":
				assert.Equal(t, 2, calls)
				assert.Equal(t, utilmetric.MetricData{}, data)
			default:
				assert.NotErrorIs(t, err, utilmetric.ErrPodMetricUnsupported)
			}
		})
	}
}

func TestMetricsReaderPodMetricContract(t *testing.T) {
	t.Parallel()
	for _, reader := range []types.MetricsReader{
		&MetricsFetcherImpl{podFetcher: &pod.PodFetcherStub{}},
		NewFakeMetricsFetcher(metrics.DummyMetrics{}),
	} {
		for _, p := range []*v1.Pod{nil, {ObjectMeta: metav1.ObjectMeta{UID: "ordinary"}}} {
			data, handled, err := reader.GetPodMetric(p, consts.MetricMemRssContainer, -1)
			require.NoError(t, err)
			assert.False(t, handled)
			assert.Equal(t, utilmetric.MetricData{}, data)
		}
	}
}

func TestMetricsReaderMetricNameMapping(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		alias  string
		metric string
		numaID int
	}{
		{name: "cpu usage", alias: consts.MetricCPUUsageContainer, metric: consts.MetricCPUUsageCgroup, numaID: -1},
		{name: "cpu user", alias: consts.MetricCPUUsageUserContainer, metric: consts.MetricCPUUsageUserCgroup, numaID: -1},
		{name: "cpu system", alias: consts.MetricCPUUsageSysContainer, metric: consts.MetricCPUUsageSysCgroup, numaID: -1},
		{name: "runnable", alias: consts.MetricCPUNrRunnableContainer, metric: consts.MetricCPUNrRunnableCgroup, numaID: -1},
		{name: "uninterruptible", alias: consts.MetricCPUNrUninterruptibleContainer, metric: consts.MetricCPUNrUninterruptibleCgroup, numaID: -1},
		{name: "io wait tasks", alias: consts.MetricCPUNrIOWaitContainer, metric: consts.MetricCPUNrIOWaitCgroup, numaID: -1},
		{name: "load1", alias: consts.MetricLoad1MinContainer, metric: consts.MetricLoad1MinCgroup, numaID: -1},
		{name: "load5", alias: consts.MetricLoad5MinContainer, metric: consts.MetricLoad5MinCgroup, numaID: -1},
		{name: "load15", alias: consts.MetricLoad15MinContainer, metric: consts.MetricLoad15MinCgroup, numaID: -1},
		{name: "usage", alias: consts.MetricMemUsageContainer, metric: consts.MetricMemUsageCgroup, numaID: -1},
		{name: "rss", alias: consts.MetricMemRssContainer, metric: consts.MetricMemRssCgroup, numaID: -1},
		{name: "cache", alias: consts.MetricMemCacheContainer, metric: consts.MetricMemCacheCgroup, numaID: -1},
		{name: "shmem", alias: consts.MetricMemShmemContainer, metric: consts.MetricMemShmemCgroup, numaID: -1},
		{name: "numa total", alias: consts.MetricsMemTotalPerNumaContainer, metric: consts.MetricsMemTotalPerNumaCgroup, numaID: 0},
		{name: "numa file", alias: consts.MetricsMemFilePerNumaContainer, metric: consts.MetricsMemFilePerNumaCgroup, numaID: 0},
		{name: "numa anon", alias: consts.MetricsMemAnonPerNumaContainer, metric: consts.MetricsMemAnonPerNumaCgroup, numaID: 0},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			target := types.PodMetricTarget{Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "target"}}, CgroupPath: "/target", MappingRevision: 1}
			store := utilmetric.NewMetricStore()
			sample := utilmetric.PodMetricData{CgroupPath: target.CgroupPath, MappingRevision: target.MappingRevision}
			values := map[string]utilmetric.MetricData{tc.metric: {Value: 7}}
			if tc.numaID < 0 {
				sample.Metrics = values
			} else {
				sample.NumaMetrics = map[int]map[string]utilmetric.MetricData{tc.numaID: values}
			}
			store.SetPodMetrics(string(target.Pod.UID), sample)
			var f types.MetricsReader = &MetricsFetcherImpl{podFetcher: &wholePodFetcher{target: target}, metricStore: store, checkMetricDataExpire: checkMetricDataExpireFunc(0)}
			for _, name := range []string{tc.alias, tc.metric} {
				data, handled, err := f.GetPodMetric(target.Pod, name, tc.numaID)
				require.NoError(t, err)
				assert.True(t, handled)
				assert.Equal(t, float64(7), data.Value)
			}
			_, handled, err := f.GetPodMetric(target.Pod, consts.MetricMemLimitCgroup, -1)
			assert.True(t, handled)
			assert.ErrorIs(t, err, utilmetric.ErrPodMetricUnsupported)
		})
	}
}

func TestGetPodMetricMissingNUMAMemoryPreservesScalarMemory(t *testing.T) {
	t.Parallel()
	target := types.PodMetricTarget{Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "target"}}, CgroupPath: "/target", MappingRevision: 1}
	store := utilmetric.NewMetricStore()
	store.SetPodMetrics("target", utilmetric.PodMetricData{
		CgroupPath: target.CgroupPath, MappingRevision: target.MappingRevision,
		Metrics: map[string]utilmetric.MetricData{consts.MetricMemRssCgroup: {Value: 7}},
	})
	f := &MetricsFetcherImpl{podFetcher: &wholePodFetcher{target: target}, metricStore: store, checkMetricDataExpire: checkMetricDataExpireFunc(0)}
	for _, name := range []string{consts.MetricsMemTotalPerNumaContainer, consts.MetricsMemTotalPerNumaCgroup} {
		_, handled, err := f.GetPodMetric(target.Pod, name, 0)
		assert.True(t, handled)
		require.Error(t, err)
		assert.False(t, errors.Is(err, utilmetric.ErrPodMetricUnsupported))
	}
	data, handled, err := f.GetPodMetric(target.Pod, consts.MetricMemRssContainer, -1)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, float64(7), data.Value)
}

func TestPodMetricNameSupportDoesNotDependOnSample(t *testing.T) {
	t.Parallel()
	for _, populated := range []bool{false, true} {
		populated := populated
		t.Run(fmt.Sprint(populated), func(t *testing.T) {
			t.Parallel()
			target := types.PodMetricTarget{Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "target"}}, CgroupPath: "/target", MappingRevision: 1}
			store := utilmetric.NewMetricStore()
			if populated {
				store.SetPodMetrics("target", utilmetric.PodMetricData{CgroupPath: "/target", MappingRevision: 1, Metrics: map[string]utilmetric.MetricData{
					consts.MetricMemLimitCgroup: {Value: 9},
				}})
			}
			f := &MetricsFetcherImpl{podFetcher: &wholePodFetcher{target: target}, metricStore: store, checkMetricDataExpire: checkMetricDataExpireFunc(0)}
			for _, name := range []string{consts.MetricMemLimitContainer, consts.MetricMemLimitCgroup, "", "mem.rss", "mem.rss.container.extra", "mem.container.rss.container", "mem.rss.cgroup.container", "mem.rss.container.container"} {
				data, handled, err := f.GetPodMetric(target.Pod, name, -1)
				assert.True(t, handled)
				assert.ErrorIs(t, err, utilmetric.ErrPodMetricUnsupported)
				assert.Equal(t, utilmetric.MetricData{}, data)
			}
			for _, name := range []string{consts.MetricMemRssContainer, consts.MetricMemRssCgroup} {
				_, handled, err := f.GetPodMetric(target.Pod, name, -1)
				assert.True(t, handled)
				require.Error(t, err)
				assert.NotErrorIs(t, err, utilmetric.ErrPodMetricUnsupported)
			}
		})
	}
}

func TestMetricsReaderBoundMappingAndCapabilities(t *testing.T) {
	t.Parallel()
	target := types.PodMetricTarget{Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "target"}}, CgroupPath: "/target", MappingRevision: 1}
	provider := &wholePodFetcher{target: target}
	store := utilmetric.NewMetricStore()
	now := time.Now()
	store.SetPodMetrics("target", utilmetric.PodMetricData{CgroupPath: "/target", MappingRevision: 1, Metrics: map[string]utilmetric.MetricData{consts.MetricMemRssCgroup: {Value: 7, Time: &now}}, NumaMetrics: map[int]map[string]utilmetric.MetricData{0: {consts.MetricsMemTotalPerNumaCgroup: {Value: 9, Time: &now}}}})
	f := &MetricsFetcherImpl{podFetcher: provider, metricStore: store, checkMetricDataExpire: checkMetricDataExpireFunc(time.Minute)}
	data, handled, err := f.GetPodMetric(target.Pod, consts.MetricMemRssContainer, -1)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, float64(7), data.Value)
	data, _, err = f.GetPodMetric(target.Pod, consts.MetricsMemTotalPerNumaContainer, 0)
	require.NoError(t, err)
	assert.Equal(t, float64(9), data.Value)
	for _, name := range []string{consts.MetricCPUUsageRatioContainer, consts.MetricsCPUUsageNUMAContainer, consts.MetricsContainerRootfsUsed, consts.MetricMemTCPLimitContainer, consts.MetricMemLimitContainer} {
		_, handled, err = f.GetPodMetric(target.Pod, name, -1)
		assert.True(t, handled)
		assert.ErrorIs(t, err, utilmetric.ErrPodMetricUnsupported)
	}
	_, _, err = f.GetPodMetric(target.Pod, consts.MetricMemCacheContainer, -1)
	require.Error(t, err)
	assert.False(t, errors.Is(err, utilmetric.ErrPodMetricUnsupported))
	provider.err = errors.New("source failed")
	_, handled, err = f.GetPodMetric(target.Pod, consts.MetricMemRssContainer, -1)
	assert.True(t, handled)
	assert.ErrorIs(t, err, provider.err)
	provider.err = nil
	provider.target.MappingRevision = 2
	_, _, err = f.GetPodMetric(target.Pod, consts.MetricMemRssContainer, -1)
	require.Error(t, err)
	provider.target.MappingRevision = 1
	resolveCalls := 0
	provider.onResolve = func() {
		resolveCalls++
		if resolveCalls == 2 {
			provider.target.MappingRevision = 2
		}
	}
	_, handled, err = f.GetPodMetric(target.Pod, consts.MetricMemRssContainer, -1)
	require.Error(t, err)
	assert.True(t, handled)
	provider.onResolve = nil
	_, handled, err = f.GetPodMetric(&v1.Pod{}, consts.MetricMemRssContainer, -1)
	require.NoError(t, err)
	assert.False(t, handled)
}
