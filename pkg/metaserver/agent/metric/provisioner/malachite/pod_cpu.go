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

	malachitetypes "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/provisioner/malachite/types"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
)

func (m *MalachiteMetricsProvisioner) preparePodCPUMetrics(target types.PodMetricTarget) func(*malachitetypes.MalachiteCgroupInfo, *podMetricWriter) error {
	// Malachite's Cpuacct response supplies usage, task counts and load. Check
	// the accounting hierarchy, not the v1 cpu scheduling controller.
	// GetAbsCgroupPath uses the unified root in v2; in v1, combined mounts still
	// require the expected cpuacct path to exist (for example through a symlink).
	checkIdentity := m.prepareCgroupIdentityCheck(common.GetAbsCgroupPath(common.CgroupSubsysCPUAcct, target.CgroupPath))
	return func(stats *malachitetypes.MalachiteCgroupInfo, sample *podMetricWriter) error {
		if err := checkIdentity(); err != nil {
			return err
		}
		if !hasCgroupCPUData(stats) {
			return fmt.Errorf("metric target %s CPU data unavailable", target.Pod.UID)
		}
		// Scalar usage is measured in cores, not a quota-normalized ratio.
		// Per-NUMA rates need a previous sample bound to both controller identity
		// and mapping revision; do not reuse the path-only static cgroup history.
		processor := cgroupMetricProcessor{writer: sample}
		processor.processCgroupCPUScalarData(target.CgroupPath, stats)
		return nil
	}
}

func hasCgroupCPUData(stats *malachitetypes.MalachiteCgroupInfo) bool {
	if stats == nil {
		return false
	}
	if stats.CgroupType == "V1" {
		return stats.V1 != nil && stats.V1.Cpu != nil
	}
	if stats.CgroupType == "V2" {
		return stats.V2 != nil && stats.V2.Cpu != nil
	}
	return false
}
