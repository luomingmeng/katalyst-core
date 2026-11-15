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
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubewharf/katalyst-core/pkg/consts"
	mt "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/provisioner/malachite/types"
	metrictypes "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

func TestPreparePodMemoryMetricsRejectsMissingData(t *testing.T) {
	t.Parallel()
	for _, stats := range []*mt.MalachiteCgroupInfo{nil, {CgroupType: "V1", V1: &mt.MalachiteCgroupV1Info{}}, {CgroupType: "V2", V2: &mt.MalachiteCgroupV2Info{}}} {
		directory := t.TempDir()
		m := &MalachiteMetricsProvisioner{cgroupStatFn: func(string) (os.FileInfo, error) { return os.Stat(directory) }}
		target := metrictypes.PodMetricTarget{Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "target"}}, CgroupPath: "/target"}
		sample := &podMetricWriter{PodMetricData: utilmetric.PodMetricData{Metrics: make(map[string]utilmetric.MetricData), NumaMetrics: make(map[int]map[string]utilmetric.MetricData)}}
		err := m.preparePodMemoryMetrics(target)(stats, sample)
		assert.Error(t, err)
		assert.Empty(t, sample.Metrics)
	}
}

func TestPodNumaValidationAndV2Formula(t *testing.T) {
	t.Parallel()
	m := &MalachiteMetricsProvisioner{}
	stats := &mt.MalachiteCgroupInfo{CgroupType: "V2", V2: &mt.MalachiteCgroupV2Info{Memory: &mt.MemoryCgDataV2{MemNumaStats: map[string]mt.NumaStatsV2{"N0": {Anon: 2, File: 3, Unevictable: 4, Shmem: 3}}}}}
	require.NoError(t, m.validatePodNUMAMemoryStats(stats))
	sample := &podMetricWriter{PodMetricData: utilmetric.PodMetricData{Metrics: make(map[string]utilmetric.MetricData), NumaMetrics: make(map[int]map[string]utilmetric.MetricData)}}
	processor := cgroupMetricProcessor{writer: sample}
	processor.processCgroupPerNumaMemoryData("/target", stats)
	assert.Equal(t, float64(9), sample.NumaMetrics[0][consts.MetricsMemTotalPerNumaCgroup].Value)
	stats.V2.Memory.MemNumaStats = map[string]mt.NumaStatsV2{"N-1": {}}
	require.Error(t, m.validatePodNUMAMemoryStats(stats))
	stats.V2.Memory.MemNumaStats = map[string]mt.NumaStatsV2{"N0": {Anon: math.MaxUint64, File: 1}}
	require.Error(t, m.validatePodNUMAMemoryStats(stats))
	stats.CgroupType = "V1"
	stats.V1 = &mt.MalachiteCgroupV1Info{Memory: &mt.MemoryCgDataV1{NumaStats: []mt.NumaStatsV1{{NumaName: "N0"}, {NumaName: "N0"}}}}
	require.Error(t, m.validatePodNUMAMemoryStats(stats))
	stats.V1.Memory.NumaStats = []mt.NumaStatsV1{{NumaName: "N0", HierarchicalTotal: math.MaxUint64}}
	require.Error(t, m.validatePodNUMAMemoryStats(stats))
}
