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
	"fmt"
	"math"
	"strconv"

	malachitetypes "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/provisioner/malachite/types"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
)

// Resource-specific preparation checks controller identity around the shared
// read. CPU/IO extensions can follow this boundary while reusing the response
// supplied by collectPodMetrics; they must not perform another raw read.
func (m *MalachiteMetricsProvisioner) preparePodMemoryMetrics(target types.PodMetricTarget) func(*malachitetypes.MalachiteCgroupInfo, *podMetricWriter) error {
	checkIdentity := m.prepareCgroupIdentityCheck(common.GetAbsCgroupPath(common.CgroupSubsysMemory, target.CgroupPath))
	return func(stats *malachitetypes.MalachiteCgroupInfo, sample *podMetricWriter) error {
		if err := checkIdentity(); err != nil {
			return err
		}
		if !hasCgroupMemoryData(stats) {
			return fmt.Errorf("metric target %s memory data unavailable", target.Pod.UID)
		}
		processor := cgroupMetricProcessor{writer: sample}
		processor.processCgroupMemoryData(target.CgroupPath, stats)
		if err := m.validatePodNUMAMemoryStats(stats); err != nil {
			return err
		}
		processor.processCgroupPerNumaMemoryData(target.CgroupPath, stats)
		return nil
	}
}

func (m *MalachiteMetricsProvisioner) validatePodNUMAMemoryStats(stats *malachitetypes.MalachiteCgroupInfo) error {
	seen := make(map[int]bool)
	checkID := func(name string) error {
		if len(name) < 2 || name[0] != 'N' {
			return fmt.Errorf("invalid NUMA name %s", name)
		}
		id, err := strconv.Atoi(name[1:])
		if err != nil || id < 0 || strconv.Itoa(id) != name[1:] || seen[id] {
			return fmt.Errorf("invalid or duplicate NUMA name %s", name)
		}
		if m.machineInfo != nil && m.machineInfo.CPUTopology != nil && !m.machineInfo.CPUDetails.NUMANodes().Contains(id) {
			return fmt.Errorf("unknown NUMA ID %d", id)
		}
		seen[id] = true
		return nil
	}
	if stats.CgroupType == "V1" {
		if len(stats.V1.Memory.NumaStats) == 0 {
			return fmt.Errorf("NUMA memory data missing")
		}
		for _, data := range stats.V1.Memory.NumaStats {
			if err := checkID(data.NumaName); err != nil {
				return err
			}
			if data.HierarchicalTotal > math.MaxUint64>>pageShift || data.HierarchicalFile > math.MaxUint64>>pageShift || data.HierarchicalAnon > math.MaxUint64>>pageShift {
				return fmt.Errorf("NUMA memory conversion overflow")
			}
		}
	} else {
		if len(stats.V2.Memory.MemNumaStats) == 0 {
			return fmt.Errorf("NUMA memory data missing")
		}
		for name, data := range stats.V2.Memory.MemNumaStats {
			if err := checkID(name); err != nil {
				return err
			}
			if data.Anon > math.MaxUint64-data.File || data.Anon+data.File > math.MaxUint64-data.Unevictable {
				return fmt.Errorf("NUMA memory total overflow")
			}
		}
	}
	return nil
}

func hasCgroupMemoryData(stats *malachitetypes.MalachiteCgroupInfo) bool {
	if stats == nil {
		return false
	}
	if stats.CgroupType == "V1" {
		return stats.V1 != nil && stats.V1.Memory != nil
	}
	if stats.CgroupType == "V2" {
		return stats.V2 != nil && stats.V2.Memory != nil
	}
	return false
}
