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
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

func TestPodCPUCollectionSharedReadAndResourceIsolation(t *testing.T) {
	t.Parallel()
	for _, version := range []string{"V1", "V2"} {
		version := version
		t.Run(version, func(t *testing.T) {
			t.Parallel()
			f := &metricTargetFetcher{target: metrictypes.PodMetricTarget{Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "target", Name: "workload", Namespace: "default"}, Spec: v1.PodSpec{Containers: []v1.Container{{Name: "one"}, {Name: "two"}}}}, CgroupPath: "/target", MappingRevision: 1}}
			updated := time.Now().Unix()
			var raw []byte
			var err error
			if version == "V1" {
				subsys := mt.SubSystemGroupsV1{}
				subsys.Cpuacct.V1.CPUData = mt.CPUCgDataV1{CPUUsageRatio: 2.5, PercpuUsage: []uint64{1000000000, 2000000000}, CPUUserUsageRatio: 1.5, CPUSysUsageRatio: 1, TaskNrRunning: 4, TaskNrUninterruptible: 5, TaskNrIoWait: 6, Load: mt.Load{One: 7, Five: 8, Fifteen: 9}, UpdateTime: updated, CfsPeriodUs: 100000, CfsQuotaUs: 200000}
				subsys.Memory.V1.MemoryV1Data = mt.MemoryCgDataV1{TotalRss: 12, UpdateTime: updated, NumaStats: []mt.NumaStatsV1{{NumaName: "N0", HierarchicalTotal: 1}}}
				raw, err = json.Marshal(subsys)
			} else {
				subsys := mt.SubSystemGroupsV2{}
				subsys.Cpuacct.V2.CPUData = mt.CPUCgDataV2{CPUUsageRatio: 2.5, PercpuUsage: []uint64{1000000000, 2000000000}, CPUUserUsageRatio: 1.5, CPUSysUsageRatio: 1, TaskNrRunning: 4, TaskNrUninterruptible: 5, TaskNrIoWait: 6, Load: mt.Load{One: 7, Five: 8, Fifteen: 9}, UpdateTime: updated, MaxPeriod: 100000, Max: 200000}
				subsys.Memory.V2.MemoryData = mt.MemoryCgDataV2{MemStats: mt.MemStatsV2{Anon: 12}, UpdateTime: updated, MemNumaStats: map[string]mt.NumaStatsV2{"N0": {Anon: 1}}}
				raw, err = json.Marshal(subsys)
			}
			require.NoError(t, err)
			var reads, requestFailure int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&reads, 1)
				if atomic.LoadInt32(&requestFailure) != 0 {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				assert.NoError(t, json.NewEncoder(w).Encode(mt.MalachiteCgroupResponse{Data: mt.CgroupDataInner{CgroupType: version, SubSystemGroups: raw}}))
			}))
			defer server.Close()
			c := client.NewMalachiteClient(f, metrics.DummyMetrics{})
			c.SetURL(map[string]string{client.CgroupResource: server.URL})
			store := utilmetric.NewMetricStore()
			directory, other := t.TempDir(), t.TempDir()
			cpuPath := common.GetAbsCgroupPath(common.CgroupSubsysCPUAcct, "/target")
			memoryPath := common.GetAbsCgroupPath(common.CgroupSubsysMemory, "/target")
			m := &MalachiteMetricsProvisioner{podFetcher: f, malachiteClient: c, metricStore: store, cpuToNumaMap: map[int]int{0: 0, 1: 1}}
			expected := map[string]float64{consts.MetricCPUUsageCgroup: 2.5, consts.MetricCPUUsageUserCgroup: 1.5, consts.MetricCPUUsageSysCgroup: 1, consts.MetricCPUNrRunnableCgroup: 4, consts.MetricCPUNrUninterruptibleCgroup: 5, consts.MetricCPUNrIOWaitCgroup: 6, consts.MetricLoad1MinCgroup: 7, consts.MetricLoad5MinCgroup: 8, consts.MetricLoad15MinCgroup: 9}
			for round, mode := range []string{"success", "memory missing", "cpu missing", "memory changed", "cpu changed", "memory recheck failure", "cpu recheck failure", "success", "request failure", "success"} {
				controllerStats := 0
				m.cgroupStatFn = func(path string) (os.FileInfo, error) {
					if mode == "memory missing" && path == memoryPath {
						return nil, os.ErrNotExist
					}
					if mode == "cpu missing" && path == cpuPath {
						return nil, os.ErrNotExist
					}
					if (mode == "cpu changed" || mode == "cpu recheck failure") && path == cpuPath || (mode == "memory changed" || mode == "memory recheck failure") && path == memoryPath {
						controllerStats++
						if controllerStats == 2 {
							if mode == "cpu recheck failure" || mode == "memory recheck failure" {
								return nil, os.ErrNotExist
							}
							return os.Stat(other)
						}
					}
					return os.Stat(directory)
				}
				atomic.StoreInt32(&requestFailure, 0)
				if mode == "request failure" {
					atomic.StoreInt32(&requestFailure, 1)
				}
				err := m.updatePodMetrics(context.Background())
				if mode == "success" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
				assert.Equal(t, int32(round+1), atomic.LoadInt32(&reads))
				for name, value := range expected {
					data, err := store.GetPodMetric("target", "/target", 1, name, -1)
					if mode == "cpu missing" || mode == "cpu changed" || mode == "cpu recheck failure" || mode == "request failure" {
						require.Error(t, err)
						continue
					}
					require.NoError(t, err)
					assert.Equal(t, value, data.Value)
					require.NotNil(t, data.Time)
					assert.Equal(t, updated, data.Time.Unix())
				}
				for _, numaID := range []int{0, 1} {
					for _, name := range []string{consts.MetricsCPUUsageCountNUMAContainer, consts.MetricsCPUUsageNUMAContainer} {
						_, err := store.GetPodMetric("target", "/target", 1, name, numaID)
						require.Error(t, err)
						_, err = store.GetCgroupNumaMetric("/target", numaID, name)
						require.Error(t, err)
					}
				}
				_, err = store.GetPodMetric("target", "/target", 1, consts.MetricMemRssCgroup, -1)
				if mode == "memory missing" || mode == "memory changed" || mode == "memory recheck failure" || mode == "request failure" {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestStaticCPUProcessorRetainsNumaHistory(t *testing.T) {
	t.Parallel()
	for _, version := range []string{"V1", "V2"} {
		store := utilmetric.NewMetricStore()
		m := &MalachiteMetricsProvisioner{metricStore: store, cpuToNumaMap: map[int]int{0: 0, 1: 0, 2: 1}}
		stats := &mt.MalachiteCgroupInfo{CgroupType: version}
		for round := int64(1); round <= 2; round++ {
			if version == "V1" {
				stats.V1 = &mt.MalachiteCgroupV1Info{Cpu: &mt.CPUCgDataV1{UpdateTime: round, PercpuUsage: []uint64{uint64(round) * 1000000000, uint64(round) * 2000000000, uint64(round) * 4000000000}, CfsPeriodUs: 100000}}
			} else {
				stats.V2 = &mt.MalachiteCgroupV2Info{Cpu: &mt.CPUCgDataV2{UpdateTime: round, PercpuUsage: []uint64{uint64(round) * 1000000000, uint64(round) * 2000000000, uint64(round) * 4000000000}, MaxPeriod: 100000}}
			}
			m.processCgroupCPUData("/static", stats)
			for numaID, expectedRate := range map[int]float64{0: 3, 1: 4} {
				count, err := store.GetCgroupNumaMetric("/static", numaID, consts.MetricsCPUUsageCountNUMAContainer)
				require.NoError(t, err)
				assert.Equal(t, expectedRate*float64(round)*1000000000, count.Value)
				assert.Equal(t, round, count.Time.Unix())
				data, err := store.GetCgroupNumaMetric("/static", numaID, consts.MetricsCPUUsageNUMAContainer)
				if round == 1 {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					assert.Equal(t, expectedRate, data.Value)
					assert.Equal(t, round, data.Time.Unix())
				}
			}
		}
	}
}

func TestPodCPUDataMissing(t *testing.T) {
	t.Parallel()
	for _, stats := range []*mt.MalachiteCgroupInfo{nil, {CgroupType: "V1", V1: &mt.MalachiteCgroupV1Info{}}, {CgroupType: "V2", V2: &mt.MalachiteCgroupV2Info{}}} {
		dir := t.TempDir()
		m := &MalachiteMetricsProvisioner{cgroupStatFn: func(string) (os.FileInfo, error) { return os.Stat(dir) }}
		target := metrictypes.PodMetricTarget{Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "target"}}, CgroupPath: "/target"}
		sample := &podMetricWriter{PodMetricData: utilmetric.PodMetricData{Metrics: make(map[string]utilmetric.MetricData)}}
		require.Error(t, m.preparePodCPUMetrics(target)(stats, sample))
		assert.Empty(t, sample.Metrics)
	}
}
