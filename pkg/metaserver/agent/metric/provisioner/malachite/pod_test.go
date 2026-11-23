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

package malachite

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/provisioner/malachite/client"
	mt "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/provisioner/malachite/types"
	metrictypes "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

type metricTargetFetcher struct {
	pod.PodFetcherStub
	target   metrictypes.PodMetricTarget
	listErr  error
	empty    bool
	revision uint64
}

func (f *metricTargetFetcher) GetPodMetricTargets(context.Context) ([]metrictypes.PodMetricTarget, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	if f.empty {
		return nil, nil
	}
	return []metrictypes.PodMetricTarget{f.target}, nil
}

func (f *metricTargetFetcher) ResolvePodMetricTarget(context.Context, *v1.Pod) (metrictypes.PodMetricTarget, bool, error) {
	target := f.target
	if f.revision != 0 {
		target.MappingRevision = f.revision
	}
	return target, true, f.listErr
}

func TestPodMetricMemoryFailureDoesNotAbortSharedRead(t *testing.T) {
	t.Parallel()
	f := &metricTargetFetcher{target: metrictypes.PodMetricTarget{Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "target"}}, CgroupPath: "/target", MappingRevision: 1}}
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		assert.NoError(t, json.NewEncoder(w).Encode(mt.MalachiteCgroupResponse{Data: mt.CgroupDataInner{CgroupType: "V1", SubSystemGroups: json.RawMessage(`{}`)}}))
	}))
	defer server.Close()
	c := client.NewMalachiteClient(f, metrics.DummyMetrics{})
	c.SetURL(map[string]string{client.CgroupResource: server.URL})
	m := &MalachiteMetricsProvisioner{podFetcher: f, metricStore: utilmetric.NewMetricStore(), malachiteClient: c, cgroupStatFn: func(string) (os.FileInfo, error) { return nil, os.ErrNotExist }}
	data, collectErr := m.collectPodMetrics(context.Background(), f, f.target, m.preparePodMemoryMetrics)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls))
	assert.ErrorIs(t, collectErr, os.ErrNotExist)
	assert.Empty(t, data.Metrics)
	m.metricStore.SetPodMetrics("target", data)
	_, err := m.metricStore.GetPodMetric("target", "/target", 1, consts.MetricMemRssCgroup, -1)
	require.Error(t, err)
	assert.False(t, errors.Is(err, os.ErrNotExist)) // the cache reports missing data, not the collection cause
}

func TestPodMetricResourcesShareReadAndIsolateMemoryFailures(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"missing directory", "changed directory", "invalid NUMA", "request failure", "changed mapping"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := &metricTargetFetcher{target: metrictypes.PodMetricTarget{Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "target"}}, CgroupPath: "/target", MappingRevision: 1}}
			subsys := mt.SubSystemGroupsV1{}
			subsys.Memory.V1.MemoryV1Data = mt.MemoryCgDataV1{TotalRss: 7, NumaStats: []mt.NumaStatsV1{{NumaName: "N-1"}}}
			subsys.Cpuacct.V1.CPUData.CPUUsageRatio = 2
			subsys.Blkio.V1.BlkIOData.BpfFsData.FsRead = 3
			raw, err := json.Marshal(subsys)
			require.NoError(t, err)
			var reads, prepared, processed int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&reads, 1)
				assert.Equal(t, int32(2), atomic.LoadInt32(&prepared))
				if mode == "request failure" {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				assert.NoError(t, json.NewEncoder(w).Encode(mt.MalachiteCgroupResponse{Data: mt.CgroupDataInner{CgroupType: "V1", SubSystemGroups: raw}}))
			}))
			defer server.Close()
			c := client.NewMalachiteClient(f, metrics.DummyMetrics{})
			c.SetURL(map[string]string{client.CgroupResource: server.URL})
			directory, other := t.TempDir(), t.TempDir()
			statsCalls := 0
			m := &MalachiteMetricsProvisioner{malachiteClient: c, cgroupStatFn: func(string) (os.FileInfo, error) {
				statsCalls++
				if mode == "missing directory" {
					return nil, os.ErrNotExist
				}
				if mode == "changed directory" && statsCalls%2 == 0 {
					return os.Stat(other)
				}
				return os.Stat(directory)
			}}
			// Test-only resource processors consume the real decoded CPU/IO fields.
			// This exercises test-only extensions; production IO remains unsupported.
			cpu := func(target metrictypes.PodMetricTarget) func(*mt.MalachiteCgroupInfo, *podMetricWriter) error {
				atomic.AddInt32(&prepared, 1)
				return func(stats *mt.MalachiteCgroupInfo, sample *podMetricWriter) error {
					atomic.AddInt32(&processed, 1)
					sample.SetCgroupMetric(target.CgroupPath, consts.MetricCPUUsageCgroup, utilmetric.MetricData{Value: stats.V1.Cpu.CPUUsageRatio})
					return nil
				}
			}
			io := func(target metrictypes.PodMetricTarget) func(*mt.MalachiteCgroupInfo, *podMetricWriter) error {
				atomic.AddInt32(&prepared, 1)
				return func(stats *mt.MalachiteCgroupInfo, sample *podMetricWriter) error {
					atomic.AddInt32(&processed, 1)
					sample.SetCgroupMetric(target.CgroupPath, consts.MetricBlkioReadIopsCgroup, utilmetric.MetricData{Value: float64(stats.V1.Blkio.BpfFsData.FsRead)})
					if mode == "changed mapping" {
						f.revision = 2
					}
					return nil
				}
			}
			store := utilmetric.NewMetricStore()
			for round := 0; round < 2; round++ {
				f.revision = 0
				atomic.StoreInt32(&prepared, 0)
				atomic.StoreInt32(&processed, 0)
				store.SetPodMetrics("target", utilmetric.PodMetricData{
					CgroupPath: "/target", MappingRevision: 1,
					Metrics: map[string]utilmetric.MetricData{
						consts.MetricMemRssCgroup: {Value: 99}, consts.MetricCPUUsageCgroup: {Value: 99}, consts.MetricBlkioReadIopsCgroup: {Value: 99},
					},
					NumaMetrics: map[int]map[string]utilmetric.MetricData{0: {consts.MetricsMemTotalPerNumaCgroup: {Value: 99}}},
				})
				data, collectErr := m.collectPodMetrics(context.Background(), f, f.target, m.preparePodMemoryMetrics, cpu, io)
				assert.Equal(t, int32(round+1), atomic.LoadInt32(&reads))
				store.SetPodMetrics("target", data)
				if mode == "request failure" || mode == "changed mapping" {
					require.Error(t, collectErr)
					assert.Empty(t, data.Metrics)
					assert.Empty(t, data.NumaMetrics)
					if mode == "request failure" {
						assert.Zero(t, atomic.LoadInt32(&processed))
					}
					for _, name := range []string{consts.MetricCPUUsageCgroup, consts.MetricBlkioReadIopsCgroup, consts.MetricMemRssCgroup} {
						_, err = store.GetPodMetric("target", "/target", 1, name, -1)
						require.Error(t, err)
					}
					_, err = store.GetPodMetric("target", "/target", 1, consts.MetricsMemTotalPerNumaCgroup, 0)
					require.Error(t, err)
					continue
				}
				require.Error(t, collectErr) // resource failure does not discard valid data
				assert.Equal(t, int32(2), atomic.LoadInt32(&processed))
				for name, expected := range map[string]float64{consts.MetricCPUUsageCgroup: 2, consts.MetricBlkioReadIopsCgroup: 3} {
					value, err := store.GetPodMetric("target", "/target", 1, name, -1)
					require.NoError(t, err)
					assert.Equal(t, expected, value.Value)
				}
				_, err = store.GetPodMetric("target", "/target", 1, consts.MetricsMemTotalPerNumaCgroup, 0)
				require.Error(t, err)
				_, err = store.GetPodMetric("target", "/target", 1, consts.MetricMemRssCgroup, -1)
				if mode == "invalid NUMA" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
			}
		})
	}
}

func TestPodMetricCollectionSingleReadAndFailure(t *testing.T) {
	t.Parallel()
	f := &metricTargetFetcher{target: metrictypes.PodMetricTarget{Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "target", Name: "workload", Namespace: "default"}, Spec: v1.PodSpec{Containers: []v1.Container{{Name: "one"}, {Name: "two"}}}}, CgroupPath: "/target", MappingRevision: 1}}
	var calls int32
	var fail int32
	var invalidNuma int32
	subsys := mt.SubSystemGroupsV1{}
	subsys.Memory.V1.MemoryV1Data = mt.MemoryCgDataV1{TotalRss: 7, TotalCache: 9, MemoryUsageInBytes: 20, UpdateTime: time.Now().Unix(), NumaStats: []mt.NumaStatsV1{{NumaName: "N0", HierarchicalTotal: 3, HierarchicalFile: 1, HierarchicalAnon: 2}}}
	raw, err := json.Marshal(subsys)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		if atomic.LoadInt32(&fail) != 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		reply := raw
		if atomic.LoadInt32(&invalidNuma) != 0 {
			invalid := subsys
			invalid.Memory.V1.MemoryV1Data.NumaStats = []mt.NumaStatsV1{{NumaName: "N-1"}}
			encoded, marshalErr := json.Marshal(invalid)
			assert.NoError(t, marshalErr)
			reply = encoded
		}
		assert.Equal(t, "/target", r.URL.Query().Get(client.CgroupPathParamKey))
		assert.NoError(t, json.NewEncoder(w).Encode(mt.MalachiteCgroupResponse{Data: mt.CgroupDataInner{CgroupType: "V1", SubSystemGroups: reply}}))
	}))
	defer server.Close()
	store := utilmetric.NewMetricStore()
	c := client.NewMalachiteClient(f, metrics.DummyMetrics{})
	c.SetURL(map[string]string{client.CgroupResource: server.URL})
	directory := t.TempDir()
	m := &MalachiteMetricsProvisioner{podFetcher: f, metricStore: store, malachiteClient: c, cgroupStatFn: func(string) (os.FileInfo, error) { return os.Stat(directory) }}
	f.PodList = []*v1.Pod{f.target.Pod}
	containers, err := c.GetAllPodContainersStats(context.Background())
	require.NoError(t, err)
	assert.Empty(t, containers.Stats)
	assert.Empty(t, containers.PodUIDSet)
	assert.Zero(t, atomic.LoadInt32(&calls))
	for round := 0; round < 2; round++ {
		require.NoError(t, m.updatePodMetrics(context.Background()))
		data, err := store.GetPodMetric("target", "/target", 1, consts.MetricMemRssCgroup, -1)
		require.NoError(t, err)
		assert.Equal(t, float64(7), data.Value)
		numa, err := store.GetPodMetric("target", "/target", 1, consts.MetricsMemTotalPerNumaCgroup, 0)
		require.NoError(t, err)
		assert.Equal(t, float64(3<<pageShift), numa.Value)
		assert.Equal(t, int32(round+1), atomic.LoadInt32(&calls))
	}
	atomic.StoreInt32(&fail, 1)
	require.Error(t, m.updatePodMetrics(context.Background()))
	_, err = store.GetPodMetric("target", "/target", 1, consts.MetricMemRssCgroup, -1)
	require.Error(t, err)
	assert.Equal(t, int32(3), atomic.LoadInt32(&calls))
	atomic.StoreInt32(&fail, 0)
	require.NoError(t, m.updatePodMetrics(context.Background()))
	atomic.StoreInt32(&invalidNuma, 1)
	require.Error(t, m.updatePodMetrics(context.Background()))
	_, err = store.GetPodMetric("target", "/target", 1, consts.MetricMemRssCgroup, -1)
	require.NoError(t, err)
	_, err = store.GetPodMetric("target", "/target", 1, consts.MetricsMemTotalPerNumaCgroup, 0)
	require.Error(t, err) // the failed NUMA result replaces the previous valid NUMA sample
	atomic.StoreInt32(&invalidNuma, 0)
	require.NoError(t, m.updatePodMetrics(context.Background()))
	_, err = store.GetPodMetric("target", "/target", 1, consts.MetricsMemTotalPerNumaCgroup, 0)
	require.NoError(t, err)
	changedFile := false
	other := t.TempDir()
	m.cgroupStatFn = func(string) (os.FileInfo, error) {
		if changedFile {
			return os.Stat(other)
		}
		changedFile = true
		return os.Stat(directory)
	}
	require.Error(t, m.updatePodMetrics(context.Background()))
	_, err = store.GetPodMetric("target", "/target", 1, consts.MetricMemRssCgroup, -1)
	require.Error(t, err)
	m.cgroupStatFn = func(string) (os.FileInfo, error) { return os.Stat(directory) }
	require.NoError(t, m.updatePodMetrics(context.Background()))
	f.listErr = errors.New("inventory failed")
	require.Error(t, m.updatePodMetrics(context.Background()))
	_, err = store.GetPodMetric("target", "/target", 1, consts.MetricMemRssCgroup, -1)
	require.NoError(t, err) // retained, but the source-bound business reader rejects this inventory
	f.listErr = nil
	f.empty = true
	require.NoError(t, m.updatePodMetrics(context.Background()))
	_, err = store.GetPodMetric("target", "/target", 1, consts.MetricMemRssCgroup, -1)
	require.Error(t, err)
}

func TestPrepareCgroupIdentityCheck(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"same directory", "prepare failure", "recheck failure", "rebuilt directory", "default stat"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			directory, other := t.TempDir(), t.TempDir()
			calls := 0
			m := &MalachiteMetricsProvisioner{cgroupStatFn: func(path string) (os.FileInfo, error) {
				assert.Equal(t, directory, path)
				calls++
				if mode == "prepare failure" || (mode == "recheck failure" && calls == 2) {
					return nil, os.ErrNotExist
				}
				if mode == "rebuilt directory" && calls == 2 {
					return os.Stat(other)
				}
				return os.Stat(path)
			}}
			if mode == "default stat" {
				m.cgroupStatFn = nil
			}
			check := m.prepareCgroupIdentityCheck(directory)
			if mode != "default stat" {
				assert.Equal(t, 1, calls) // capture must precede the shared read
			}
			err := check()
			switch mode {
			case "prepare failure", "recheck failure":
				assert.ErrorIs(t, err, os.ErrNotExist)
			case "rebuilt directory":
				require.Error(t, err)
			default:
				require.NoError(t, err)
			}
			if mode == "prepare failure" {
				assert.Equal(t, 1, calls) // a failed capture is not retried after the read
			} else if mode != "default stat" {
				assert.Equal(t, 2, calls)
			}
		})
	}
}

func TestPodMetricCollectionRejectsChangedMapping(t *testing.T) {
	t.Parallel()
	f := &metricTargetFetcher{target: metrictypes.PodMetricTarget{Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "target"}}, CgroupPath: "/target", MappingRevision: 1}, revision: 2}
	m := &MalachiteMetricsProvisioner{}
	data, err := m.collectPodMetrics(context.Background(), f, f.target)
	require.Error(t, err) // rejected before any HTTP call
	assert.Empty(t, data.Metrics)
}
